// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/main_config_bootstrap_helpers.h"

#include <seastar/core/print.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/std-compat.hh>

#include "common/ceph_argparse.h"
#include "common/config_tracker.h"
#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/fatal_signal.h"
#include "crimson/common/perf_counters_collection.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/main_config_bootstrap_helpers.h"

#include <sys/wait.h> // for waitpid()

using namespace std::literals;
using crimson::common::local_conf;
using crimson::common::sharded_conf;
using crimson::common::sharded_perf_coll;

static seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}

namespace crimson::osd {

void usage(const char* prog)
{
  std::cout << "crimson osd usage: " << prog << " -i <ID> [flags...]" << std::endl;
  generic_server_usage();
}


seastar::future<> populate_config_from_mon()
{
  logger().info("populating config from monitor");
  // i don't have any client before joining the cluster, so no need to have
  // a proper auth handler
  class DummyAuthHandler : public crimson::common::AuthHandler {
  public:
    void handle_authentication(const EntityName& name,
                               const AuthCapsInfo& caps)
    {}
  };
  return seastar::async([] {
    auto auth_handler = std::make_unique<DummyAuthHandler>();
    auto msgr = crimson::net::Messenger::create(entity_name_t::CLIENT(),
                                                "temp_mon_client",
                                                get_nonce(),
                                                true);
    crimson::mon::Client monc{*msgr, *auth_handler};
    msgr->set_auth_client(&monc);
    msgr->start({&monc}).get();
    auto stop_msgr = seastar::defer([&] {
      msgr->stop();
      msgr->shutdown().get();
    });
    monc.start().handle_exception([] (auto ep) {
      fmt::print(std::cerr, "FATAL: unable to connect to cluster: {}\n", ep);
      return seastar::make_exception_future<>(ep);
    }).get();
    auto stop_monc = seastar::defer([&] {
      monc.stop().get();
    });
    monc.sub_want("config", 0, 0);
    monc.renew_subs().get();
    // wait for monmap and config
    monc.wait_for_config().get();
    auto fsid = monc.get_fsid().to_string();
    local_conf().set_val("fsid", fsid).get();
    logger().debug("{}: got config from monitor, fsid {}", __func__, fsid);
  });
}

static tl::expected<early_config_t, int>
_get_early_config(int argc, const char *argv[])
{
  early_config_t ret;

  // pull off ceph configs the stuff from early_args
  std::vector<const char *> early_args;
  early_args.insert(
    std::end(early_args),
    argv, argv + argc);

  ret.init_params = ceph_argparse_early_args(
    early_args,
    CEPH_ENTITY_TYPE_OSD,
    &ret.cluster_name,
    &ret.conf_file_list);

  seastar::app_template::config app_cfg;
  app_cfg.name = "Crimson-startup";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));
  const char *bootstrap_args[] = { argv[0], "--smp", "1" };

  int r = app.run(
    sizeof(bootstrap_args) / sizeof(bootstrap_args[0]),
    const_cast<char**>(bootstrap_args),
    [argc, argv, &ret, &early_args] {
      return seastar::async([argc, argv, &ret, &early_args] {
	seastar::global_logger_registry().set_all_loggers_level(
	  seastar::log_level::debug);
	sharded_conf().start(
	  ret.init_params.name, ret.cluster_name).get();
	local_conf().start().get();
	auto stop_conf = seastar::deferred_stop(sharded_conf());

	sharded_perf_coll().start().get();
	auto stop_perf_coll = seastar::deferred_stop(sharded_perf_coll());

	local_conf().parse_env().get();
	local_conf().parse_argv(early_args).get();
	local_conf().parse_config_files(ret.conf_file_list).get();

	if (local_conf()->no_mon_config) {
	  logger().info("bypassing the config fetch due to --no-mon-config");
	} else {
	  populate_config_from_mon().get();
	}

        using OptionValue = std::variant<std::string, double, uint64_t>;
        // Define a structure to represent each option
        struct SeastarOption {
          std::string option_name;                     // Command-line option name
          std::string config_key;                      // Configuration key
          std::optional<OptionValue> default_value;    // Optional default value
        };

        //Define a map of options with their corresponding configuration keys and default values (if applicable)
        // In future to add an option introduce a new entry in this map with corresponding key name and value
        const std::vector<SeastarOption> seastar_options = {
        {"--cpuset", "crimson_seastar_cpu_cores", std::optional<OptionValue>("")},
        {"--smp", "crimson_seastar_num_threads", std::optional<OptionValue>(static_cast<uint64_t>(0))},
        {"--task-quota-ms", "crimson_seastar_task_quota_ms", std::optional<OptionValue>(0.5)},
        {"--io-latency-goal-ms", "crimson_seastar_io_latency_goal_ms", std::optional<OptionValue>(0.75)},
        {"--idle-poll-time-us", "crimson_seastar_idle_poll_time_us", std::optional<OptionValue>(static_cast<uint64_t>(200))}};

	// get ceph configs
	std::set_difference(
	  argv, argv + argc,
	  std::begin(early_args),
	  std::end(early_args),
	  std::back_inserter(ret.ceph_args));

	ret.early_args.insert(
	  std::end(ret.early_args),
	  std::begin(early_args),
	  std::end(early_args));

        auto found = std::find_if(
              std::begin(early_args),
              std::end(early_args),
              [](auto* arg) { return "--cpuset"sv == arg; });

        bool cpuset_set = false;
        bool smp_set = false;
        std::optional<OptionValue> tmp_value;
        for (const auto& option : seastar_options) {
           std::optional<OptionValue> option_value;
           if ((found == std::end(early_args)) && option.option_name == "--cpuset") {
             tmp_value = crimson::common::get_conf<std::string>("crimson_seastar_cpu_cores");
             if (std::holds_alternative<std::string>(tmp_value.value())) {
               const std::string& str_value = std::get<std::string>(tmp_value.value());
               if (str_value != "0-0") {
                 cpuset_set = true;
                 option_value = tmp_value;
               }
             }
           }
           if ((found == std::end(early_args)) && option.option_name == "--smp" && !cpuset_set) {
             smp_set = true;
             option_value = crimson::common::get_conf<uint64_t>("crimson_seastar_num_threads");
             if (!option_value.has_value()) {
               logger().error("get_early_config: crimson_seastar_cpu_cores"
                             " or crimson_seastar_num_threads"
                             " must be set");
               ceph_abort();
             }
           }
           if (option.option_name == "--task-quota-ms") {
             option_value = crimson::common::get_conf<double>(option.config_key);
           }
           if(option.option_name == "--io-latency-goal-ms") {
            option_value = crimson::common::get_conf<double>(option.config_key);
           }
           if (option.option_name == "--idle-poll-time-us") {
            option_value = crimson::common::get_conf<uint64_t>(option.config_key);
           }
           if (!option_value) {
             logger().info("get_early_config --option_name {} is skip to set "
                           " either null or condition is not fullfill ", option.config_key);
           } else {
             logger().info(
                "get_early_config --option_name {} with --option_value {} ",
                option.config_key,
                std::visit(
                    [](auto&& val) -> std::string {
                    return fmt::format("{}", val);
                }, *option_value));

             ret.early_args.emplace_back(option.option_name);
             std::visit([&ret](auto&& value) {
                 using T = std::decay_t<decltype(value)>;
                 if constexpr (std::is_same_v<T, std::string>) {
                   // If the value is a string, add it directly
                   ret.early_args.emplace_back(value);
                 } else {
                   // For other types, convert to string
                   ret.early_args.emplace_back(std::to_string(value));
                 }
                }, *option_value);
           }
        }

        if (cpuset_set) {
          ret.early_args.emplace_back("--thread-affinity");
          ret.early_args.emplace_back("1");
        }
        if (smp_set) {
          ret.early_args.emplace_back("--thread-affinity");
          ret.early_args.emplace_back("0");
        }
	return 0;
      });
    });
  if (r < 0) {
    return tl::unexpected(r);
  }
  return ret;
}

/* get_early_config handles obtaining config parameters required prior
 * to reactor startup.  Most deployment mechanisms (cephadm for one)
 * rely on pulling configs from the monitor rather than shipping around
 * config files, so this process needs to support pulling config options
 * from the monitors.
 *
 * Of particular interest are config params related to the seastar
 * reactor itself which can't be modified after the reactor has been
 * started -- like the number of cores to use (smp::count).  Contacting
 * the monitors, however, requires a MonClient, which in turn needs a
 * running reactor.
 *
 * Unfortunately, seastar doesn't clean up thread local state
 * associated with seastar::smp task queues etc, so we can't
 * start a reactor, stop it, and restart it in the same thread
 * without an impractical amount of cleanup in seastar.
 *
 * More unfortunately, starting a reactor in a seperate thread
 * and then joining the thread still doesn't avoid all global state,
 * I observed tasks from the previous reactor incarnation nevertheless
 * continuing to run in the new one resulting in a crash as they access
 * freed memory.
 *
 * The approach taken here, therefore, is to actually fork, start a
 * reactor in the child process, encode the resulting early_config_t,
 * and send it back to the parent process.
 */
tl::expected<early_config_t, int>
get_early_config(int argc, const char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage(argv[0]);
    exit(0);
  }
  int pipes[2];
  int r = pipe2(pipes, 0);
  if (r < 0) {
    std::cerr << "get_early_config: failed to create pipes: "
	      << -errno << std::endl;
    return tl::unexpected(-errno);
  }

  pid_t worker = fork();
  if (worker < 0) {
    close(pipes[0]);
    close(pipes[1]);
    std::cerr << "get_early_config: failed to fork: "
	      << -errno << std::endl;
    return tl::unexpected(-errno);
  } else if (worker == 0) { // child
    close(pipes[0]);
    auto ret = _get_early_config(argc, argv);
    if (ret.has_value()) {
      bufferlist bl;
      ::encode(ret.value(), bl);
      r = bl.write_fd(pipes[1]);
      close(pipes[1]);
      if (r < 0) {
	std::cerr << "get_early_config: child failed to write_fd: "
		  << r << std::endl;
	exit(-r);
      } else {
	exit(0);
      }
    } else {
      std::cerr << "get_early_config: child failed: "
		<< -ret.error() << std::endl;
      exit(-ret.error());
    }
    return tl::unexpected(-1);
  } else { // parent
    close(pipes[1]);

    bufferlist bl;
    early_config_t ret;
    bool have_data = false;
    while ((r = bl.read_fd(pipes[0], 1024)) > 0) {
      have_data = true;
    }
    close(pipes[0]);

    int status;
    waitpid(worker, &status, 0);

    // One of the parameters was taged as exit(0) in the child process
    // so we need to check if we should exit here
    if (!have_data && WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      exit(0);
    }
    if (r < 0) {
      std::cerr << "get_early_config: parent failed to read from pipe: "
		<< r << std::endl;
      return tl::unexpected(r);
    }
    try {
      auto bliter = bl.cbegin();
      ::decode(ret, bliter);
      return ret;
    } catch (...) {
      std::cerr << "get_early_config: parent failed to decode" << std::endl;
      return tl::unexpected(-EINVAL);
    }
  }
}

}
