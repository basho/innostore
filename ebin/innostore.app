{application, innostore,
 [{description, "Simple Erlang API to Embedded Inno DB"},
  {vsn, "0.1"},
  {modules, [
             innostore
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

