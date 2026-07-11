#include "client.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>

#include <boost/asio/append.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/generic/datagram_protocol.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/system/system_error.hpp>

#include <curl/curl.h>

#include "common/async/service.h"
#include "common/dout.h"

#if !CURL_AT_LEAST_VERSION(7, 17, 1)
#error "requires libcurl >= 7.17.1 for CURLOPT_OPENSOCKETFUNCTION"
#endif

namespace rgw::curl {

boost::system::error_category& easy_category()
{
  static struct category : boost::system::error_category {
    virtual ~category() {}
    const char* name() const noexcept override { return "curl easy"; }
    std::string message(int code) const override {
      return ::curl_easy_strerror(static_cast<CURLcode>(code));
    }
  } instance;
  return instance;
}

boost::system::error_category& multi_category()
{
  static struct category : boost::system::error_category {
    virtual ~category() {}
    const char* name() const noexcept override { return "curl multi"; }
    std::string message(int code) const override {
      return ::curl_multi_strerror(static_cast<CURLMcode>(code));
    }
  } instance;
  return instance;
}


void easy_deleter::operator()(CURL* p) { ::curl_easy_cleanup(p); }

easy_ptr easy_init(error_code& ec)
{
  auto easy = easy_ptr{::curl_easy_init()};
  if (!easy) {
    ec.assign(CURLE_OUT_OF_MEMORY, easy_category());
  }
  return easy;
}

easy_ptr easy_init()
{
  error_code ec;
  auto easy = easy_init(ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_easy_init");
  }
  return easy;
}

template <typename T>
void easy_setopt(CURL* easy, CURLoption option, T&& value, error_code& ec)
{
  CURLcode code = ::curl_easy_setopt(easy, option, std::forward<T>(value));
  if (code != CURLE_OK) {
    ec.assign(code, easy_category());
  }
}

template <typename T>
void easy_setopt(CURL* easy, CURLoption option, T&& value)
{
  error_code ec;
  easy_setopt(easy, option, std::forward<T>(value), ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_easy_setopt");
  }
}


void multi_deleter::operator()(CURLM* p) { ::curl_multi_cleanup(p); }

multi_ptr multi_init(error_code& ec)
{
  auto multi = multi_ptr{::curl_multi_init()};
  if (!multi) {
    ec.assign(CURLE_OUT_OF_MEMORY, multi_category());
  }
  return multi;
}

multi_ptr multi_init()
{
  error_code ec;
  auto multi = multi_init(ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_multi_init");
  }
  return multi;
}

template <typename T>
void multi_setopt(CURLM* multi, CURLMoption option, T&& value, error_code& ec)
{
  CURLMcode mcode = ::curl_multi_setopt(multi, option, std::forward<T>(value));
  if (mcode != CURLM_OK) {
    ec.assign(mcode, multi_category());
  }
}

template <typename T>
void multi_setopt(CURLM* multi, CURLMoption option, T&& value)
{
  error_code ec;
  multi_setopt(multi, option, std::forward<T>(value), ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_multi_setopt");
  }
}


void slist_deleter::operator()(curl_slist* p) { ::curl_slist_free_all(p); }

void slist_append(slist_ptr& p, const char* str, error_code& ec)
{
  if (curl_slist* tmp = ::curl_slist_append(p.get(), str); tmp) {
    (void) p.release(); // will be freed by curl_slist_free_all(tmp)
    p.reset(tmp);
  } else {
    ec.assign(CURLE_OUT_OF_MEMORY, easy_category());
  }
}

void slist_append(slist_ptr& p, const char* str)
{
  error_code ec;
  slist_append(p, str, ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_slist_append");
  }
}


// libcurl callback functions
static curl_socket_t opensocket_callback(void* user, curlsocktype purpose,
                                         curl_sockaddr* address);
static int closesocket_callback(void* user, curl_socket_t fd);
static int socket_callback(CURL* easy, curl_socket_t fd, int what,
                           void* user, void* socket);
static int timer_callback(CURLM* multi, long timeout_ms, void* user);

// route libcurl's own connection trace into the rgw log
static int debug_callback(CURL*, curl_infotype type, char* data,
                          size_t size, void*)
{
  if (type == CURLINFO_TEXT) {
    size_t len = size;
    while (len > 0 && (data[len-1] == '\n' || data[len-1] == '\r')) {
      --len;
    }
    lsubdout(g_ceph_context, rgw, 20) << "curl: "
        << std::string_view{data, len} << dendl;
  }
  return 0;
}


// This implementation uses the 'multi_socket' flavor of the libcurl multi API:
// https://curl.se/libcurl/c/libcurl-multi.html
//
// By providing our own CURLMOPT_TIMERFUNCTION AND CURLMOPT_SOCKETFUNCTION, we
// can do all necessary polling and waiting on the given asio executor. As we
// get wakeups from asio, we call curl_multi_socket_action() to do its
// non-blocking socket i/o from within that executor.
//
// This also overrides CURLOPT_OPENSOCKETFUNCTION on each request to manage the
// pool of asio sockets.
class Client::Impl :
    public boost::intrusive_ref_counter<Impl, boost::thread_safe_counter>,
    public ceph::async::service_list_base_hook
{
 public:
  explicit Impl(executor_type ex)
    : svc(boost::asio::use_service<ceph::async::service<Impl>>(
          boost::asio::query(ex, boost::asio::execution::context))),
      ex(ex),
      timer(ex),
      multi(multi_init())
  {
    multi_setopt(multi.get(), CURLMOPT_TIMERFUNCTION, timer_callback);
    multi_setopt(multi.get(), CURLMOPT_TIMERDATA, this);
    multi_setopt(multi.get(), CURLMOPT_SOCKETFUNCTION, socket_callback);
    multi_setopt(multi.get(), CURLMOPT_SOCKETDATA, this);

    // register for service_shutdown() notifications
    svc.add(*this);
  }
  ~Impl()
  {
    svc.remove(*this);
    // curl_multi_cleanup() shuts down cached connections, which can
    // invoke the socket callback; clear both callbacks so neither can
    // resurrect a dying Impl
    error_code ec;
    multi_setopt(multi.get(), CURLMOPT_TIMERFUNCTION,
                 static_cast<curl_multi_timer_callback>(nullptr), ec);
    multi_setopt(multi.get(), CURLMOPT_SOCKETFUNCTION,
                 static_cast<curl_socket_callback>(nullptr), ec);
    // release foreign fds so ~stream_descriptor does not close them
    cancel_watches();
  }

  executor_type get_executor() const noexcept { return ex; }

  void async_perform_impl(handler_type handler, CURL* easy)
  {
    // configure the easy handle and add it to the multi handle
    error_code ec;
    add_handle(easy, ec);
    if (ec) {
      boost::asio::post(boost::asio::bind_executor(get_executor(),
          boost::asio::append(std::move(handler), ec)));
      return;
    }

    // arrange for per-op cancellation
    const uint64_t id = ++next_op_id;
    auto slot = boost::asio::get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.template emplace<op_cancellation>(this, easy, id);
    }

    // register the completion handler
    handlers.emplace(easy, op_state{std::move(handler), id});

    // kick things off if they haven't started yet
    socket_action(CURL_SOCKET_TIMEOUT, 0);
  }

  void cancel(error_code ec)
  {
    auto h = handlers.begin();
    while (h != handlers.end()) {
      auto handler = std::move(h->second.handler);
      ::curl_multi_remove_handle(multi.get(), h->first);
      h = handlers.erase(h);
      boost::asio::dispatch(boost::asio::append(std::move(handler), ec));
    }
    timer.cancel();
    // abort the socket waits so their Impl refs drop; an idle keep-alive
    // fd would otherwise pin Impl
    cancel_watches();
  }

  void service_shutdown()
  {
    auto h = handlers.begin();
    while (h != handlers.end()) {
      auto handler = std::move(h->second.handler);
      ::curl_multi_remove_handle(multi.get(), h->first);
      h = handlers.erase(h);
    }
    // the reactor services are already shut down and their waits destroyed;
    // only release the foreign fds so ~stream_descriptor does not close them
    release_watches();
  }

 private:
  ceph::async::service<Impl>& svc;
  executor_type ex;
  boost::asio::steady_timer timer;

  // a watch per fd that libcurl polls. libcurl only repeats its socket
  // callback when the events it wants change, while asio waits are one-shot,
  // so track what's armed and silence stale handlers by generation
  struct socket_watch {
    std::variant<boost::asio::generic::stream_protocol::socket,
                 boost::asio::generic::datagram_protocol::socket,
                 boost::asio::posix::stream_descriptor> io;
    int interest = 0;    // CURL_POLL_* events libcurl asked for
    int waiting = 0;     // CURL_CSELECT_* waits currently armed
    uint64_t gen = 0;
  };
  std::unordered_map<curl_socket_t, socket_watch> watches;
  uint64_t watch_gen = 0;

  // handler plus an id that guards cancellation against easy handle reuse
  struct op_state {
    handler_type handler;
    uint64_t id;
  };
  using handler_map = std::unordered_map<CURL*, op_state>;
  handler_map handlers;
  uint64_t next_op_id = 0;

  multi_ptr multi;

  // handler for per-op cancellation
  struct op_cancellation {
    boost::intrusive_ptr<Impl> impl;
    CURL* easy;
    uint64_t id;

    op_cancellation(Impl* impl, CURL* easy, uint64_t id)
      : impl(impl), easy(easy), id(id) {}

    void operator()(boost::asio::cancellation_type_t type) {
      if (type == boost::asio::cancellation_type::none) {
        return;
      }
      // cancellation fires on the emitting thread; the handler map and
      // multi handle are only safe to touch on the client's executor.
      // post, not dispatch: an inline completion could resume the waiter
      // inside emit() and destroy the signal under it
      boost::asio::post(impl->ex, [impl = impl, easy = easy, id = id] {
            auto h = impl->handlers.find(easy);
            if (h == impl->handlers.end() || h->second.id != id) {
              return;   // completed; the easy pointer may have been reused
            }
            auto ec = make_error_code(boost::asio::error::operation_aborted);
            auto handler = std::move(h->second.handler);
            ::curl_multi_remove_handle(impl->multi.get(), easy);
            impl->handlers.erase(h);
            boost::asio::dispatch(boost::asio::append(std::move(handler), ec));
          });
    }
  };

  void add_handle(CURL* easy, error_code& ec)
  {
    // capture libcurl's connection trace when rgw logging is verbose
    if (g_ceph_context &&
        g_ceph_context->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      easy_setopt(easy, CURLOPT_VERBOSE, 1L, ec);
      if (ec) {
        return;
      }
      easy_setopt(easy, CURLOPT_DEBUGFUNCTION, debug_callback, ec);
      if (ec) {
        return;
      }
    }

    // attach socket callbacks to the easy handle
    easy_setopt(easy, CURLOPT_OPENSOCKETFUNCTION, opensocket_callback, ec);
    if (ec) {
      return;
    }
    easy_setopt(easy, CURLOPT_OPENSOCKETDATA, this, ec);
    if (ec) {
      return;
    }
    easy_setopt(easy, CURLOPT_CLOSESOCKETFUNCTION, closesocket_callback, ec);
    if (ec) {
      return;
    }
    easy_setopt(easy, CURLOPT_CLOSESOCKETDATA, this, ec);
    if (ec) {
      return;
    }

    // register the easy handle with the multi handle
    CURLMcode mcode = ::curl_multi_add_handle(multi.get(), easy);
    if (mcode != CURLM_OK) {
      ec.assign(mcode, multi_category());
      return;
    }
  }

  void socket_action(curl_socket_t fd, int events)
  {
    // process the socket action as many times as necessary
    CURLMcode mcode = CURLM_OK;
    int count = 0;
    do {
      mcode = ::curl_multi_socket_action(multi.get(), fd, events, &count);
    } while (mcode == CURLM_CALL_MULTI_PERFORM);

    if (mcode != CURLM_OK) {
      // on error, all transfers must be aborted
      cancel(error_code{mcode, multi_category()});
      return;
    }

    // handle any completions
    while (CURLMsg* msg = ::curl_multi_info_read(multi.get(), &count)) {
      if (msg->msg == CURLMSG_DONE) {
        error_code ec;
        if (msg->data.result != CURLE_OK) {
          ec.assign(msg->data.result, easy_category());
        }
        auto h = handlers.find(msg->easy_handle);
        if (h != handlers.end()) {
          auto handler = std::move(h->second.handler);
          ::curl_multi_remove_handle(multi.get(), h->first);
          handlers.erase(h);
          boost::asio::dispatch(boost::asio::append(std::move(handler), ec));
        }
      }
    }
  }

  // construct and open a tcp or udp socket
  template <typename Protocol>
  curl_socket_t open_socket(const Protocol& proto)
  {
    auto socket = typename Protocol::socket{get_executor()};
    error_code ec;
    socket.open(proto, ec);
    if (ec) {
      return CURL_SOCKET_BAD;
    }
    curl_socket_t fd = socket.native_handle();
    watches.emplace(fd, socket_watch{std::move(socket)});
    return fd;
  }

  friend curl_socket_t opensocket_callback(void* user, curlsocktype purpose,
                                           curl_sockaddr* address)
  {
    auto impl = static_cast<Impl*>(user);

    // libcurl may OR SOCK_CLOEXEC/SOCK_NONBLOCK into socktype
    int socktype = address->socktype;
#ifdef SOCK_CLOEXEC
    socktype &= ~SOCK_CLOEXEC;
#endif
#ifdef SOCK_NONBLOCK
    socktype &= ~SOCK_NONBLOCK;
#endif

    if (socktype == SOCK_STREAM) {
      using protocol_type = boost::asio::generic::stream_protocol;
      return impl->open_socket(protocol_type{address->family,
                                             address->protocol});
    }
    if (socktype == SOCK_DGRAM) {
      using protocol_type = boost::asio::generic::datagram_protocol;
      return impl->open_socket(protocol_type{address->family,
                                             address->protocol});
    }
    return CURL_SOCKET_BAD;
  }

  friend int closesocket_callback(void* user, curl_socket_t fd)
  {
    auto impl = static_cast<Impl*>(user);
    // erasing the watch destroys the socket and cancels its wait
    impl->watches.erase(fd);
    return 0;
  }

  // bump the generation to drop an in-flight completion, then cancel the waits
  void retire_watch(socket_watch& w)
  {
    w.gen = ++watch_gen;
    w.interest = 0;
    error_code ec;
    std::visit([&] (auto& io) { io.cancel(ec); }, w.io);
    w.waiting = 0;
  }

  // release libcurl's foreign fds so ~stream_descriptor does not close them
  void release_watches()
  {
    for (auto i = watches.begin(); i != watches.end(); ) {
      if (auto* desc = std::get_if<boost::asio::posix::stream_descriptor>(
              &i->second.io); desc) {
        (void) desc->release();
        i = watches.erase(i);
      } else {
        ++i;
      }
    }
  }

  void cancel_watches()
  {
    for (auto& w : watches) {
      retire_watch(w.second);
    }
    release_watches();
  }

  struct socket_wait_handler {
    boost::intrusive_ptr<Impl> impl;
    curl_socket_t fd;
    int mask;
    uint64_t gen;

    socket_wait_handler(Impl* impl, curl_socket_t fd, int mask, uint64_t gen)
      : impl(impl), fd(fd), mask(mask), gen(gen) {}

    // callback for async_wait()
    void operator()(error_code ec)
    {
      auto i = impl->watches.find(fd);
      if (i == impl->watches.end() || i->second.gen != gen ||
          ec == boost::asio::error::operation_aborted) {
        return;   // watch removed, re-registered, or canceled
      }
      i->second.waiting &= ~mask;

      impl->socket_action(fd, ec ? mask | CURL_CSELECT_ERR : mask);

      // libcurl repeats its socket callback only when the events it wants
      // change, so a fired wait has to re-arm itself. socket_action() above
      // may have removed or re-registered this watch, so look it up again
      if (!ec) {
        if (auto j = impl->watches.find(fd); j != impl->watches.end()) {
          impl->arm_watch(fd, j->second);
        }
      }
    }
  };

  void arm_watch(curl_socket_t fd, socket_watch& w)
  {
    if ((w.interest & CURL_POLL_IN) && !(w.waiting & CURL_CSELECT_IN)) {
      w.waiting |= CURL_CSELECT_IN;
      std::visit([&] (auto& io) {
          io.async_wait(std::decay_t<decltype(io)>::wait_read,
                        socket_wait_handler{this, fd, CURL_CSELECT_IN, w.gen});
        }, w.io);
    }
    if ((w.interest & CURL_POLL_OUT) && !(w.waiting & CURL_CSELECT_OUT)) {
      w.waiting |= CURL_CSELECT_OUT;
      std::visit([&] (auto& io) {
          io.async_wait(std::decay_t<decltype(io)>::wait_write,
                        socket_wait_handler{this, fd, CURL_CSELECT_OUT, w.gen});
        }, w.io);
    }
  }

  friend int socket_callback(CURL* easy, curl_socket_t fd, int what,
                             void* user, void* socket)
  {
    auto impl = static_cast<Impl*>(user);
    auto i = impl->watches.find(fd);

    if (what == CURL_POLL_REMOVE) {
      if (i == impl->watches.end()) {
        return 0;
      }
      if (auto* desc = std::get_if<boost::asio::posix::stream_descriptor>(
              &i->second.io); desc) {
        // release aborts the wait and detaches libcurl's fd; erase drops it
        (void) desc->release();
        impl->watches.erase(i);
      } else {
        // the socket stays open until closesocket_callback(); stop polling it
        impl->retire_watch(i->second);
      }
      return 0;
    }

    if (i == impl->watches.end()) {
      // libcurl opened this fd itself (e.g. resolver wakeup); wait, don't own
      error_code ec;
      boost::asio::posix::stream_descriptor desc{impl->get_executor()};
      desc.assign(fd, ec);
      if (ec) {
        return -1;
      }
      i = impl->watches.emplace(fd, socket_watch{std::move(desc)}).first;
    }

    /*
     * A changed interest leaves any wait armed for the old events pending.
     * Bumping the generation retires it: when it fires, its handler sees the
     * new value and returns without acting. It stays queued on the fd until
     * the next readiness.
     */
    if (i->second.interest != what) {
      i->second.interest = what;
      i->second.waiting = 0;
      i->second.gen = ++impl->watch_gen;
    }
    impl->arm_watch(fd, i->second);
    return 0;
  }

  friend int timer_callback(CURLM* multi, long timeout_ms, void* user)
  {
    boost::intrusive_ptr impl = static_cast<Impl*>(user);
    if (timeout_ms == -1) {
      impl->timer.cancel();
      return 0;
    }
    impl->timer.expires_after(std::chrono::milliseconds(timeout_ms));
    impl->timer.async_wait([impl] (error_code ec) {
          if (!ec) {
            impl->socket_action(CURL_SOCKET_TIMEOUT, 0);
          }
        });
    return 0;
  }
};


Client::Client(executor_type ex)
  : impl(new Impl(ex))
{
}

Client::~Client()
{
  cancel();
}

auto Client::get_executor() const noexcept -> executor_type
{
  return impl->get_executor();
}

void Client::async_perform_impl(handler_type handler, CURL* easy)
{
  impl->async_perform_impl(std::move(handler), easy);
}

void Client::cancel()
{
  impl->cancel(boost::asio::error::operation_aborted);
}

} // namespace rgw::curl
