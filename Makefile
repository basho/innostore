
all:    # Innostore only works with the SMP runtime, force it on uniprocessors
	ERL_FLAGS="-smp enable" ./rebar compile eunit verbose=1

clean:
	./rebar clean

install:
	./rebar install

