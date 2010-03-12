{application, innostore,
 [{description, "Simple Erlang API to Embedded Inno DB"},
  {vsn, "9"},
  {modules, [
             innostore,
             innostore_riak
            ]},
  {applications, [
                  kernel,
                  stdlib,
                  sasl,
                  crypto
                 ]},
  {registered, []},
  {env, [
         %% Use current working directory for both log and data files by default
         {log_group_home_dir, "."},
         {data_home_dir, "."},

         {flush_log_at_trx_commit,  0},  % Flush pending log commits once per second
         {max_dirty_pages_pct,      75}, % Reduce frequency at which dirty pages are flushed
         {log_buffer_size,     8388608}
        ]}
 ]}.

