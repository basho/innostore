{application, innostore,
 [{description, "Simple Erlang API to Embedded Inno DB"},
  {vsn, "3"},
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
         {log_group_home_dir, "."},
         {data_home_dir, "."}
        ]}
 ]}.

