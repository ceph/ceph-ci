#include "common/ceph_argparse.h"
#include "common/config.h"
#include "exporter/DaemonMetricCollector.h"
#include "exporter/web_server.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <string>

#define dout_context g_ceph_context

static void usage() {
  std::cout << "usage: ceph-exporter [options]\n"
            << "options:\n"
               "  --sock-dir:     The path to ceph daemons socket files dir\n"
               "  --addrs:        Host ip address where exporter is deployed\n"
               "  --port:         Port to deploy exporter on. Default is 9926\n"
               "  --cert-file:    Path to the certificate file to use https\n"
               "  --key-file:     Path to the certificate key file to use https\n"
               "  --prio-limit:   Only perf counters greater than or equal to prio-limit are fetched. Default: 5\n"
               "  --stats-period: Time to wait before sending requests again to exporter server (seconds). Default: 5s"
            << std::endl;
  generic_server_usage();
}

int main(int argc, char **argv) {

  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON, 0);
  std::string val;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--sock-dir", (char *)NULL)) {
      cct->_conf.set_val("exporter_sock_dir", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--addrs", (char *)NULL)) {
      cct->_conf.set_val("exporter_addr", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--port", (char *)NULL)) {
      cct->_conf.set_val("exporter_http_port", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--cert-file", (char *)NULL)) {
      cct->_conf.set_val("exporter_cert_file", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--key-file", (char *)NULL)) {
      cct->_conf.set_val("exporter_key_file", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--prio-limit", (char *)NULL)) {
      cct->_conf.set_val("exporter_prio_limit", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--stats-period", (char *)NULL)) {
      cct->_conf.set_val("exporter_stats_period", val);
    } else {
      ++i;
    }
  }
  common_init_finish(g_ceph_context);

  boost::thread server_thread(web_server_thread_entrypoint);
  DaemonMetricCollector &collector = collector_instance();
  collector.main();
  server_thread.join();
}
