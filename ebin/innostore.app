{application, innostore,
 [{description, "Simple Erlang API to Embedded Inno DB"},
  {vsn, "2"},
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
  {env, []}
 ]}.

