REBAR ?= rebar3
PROJECT := rmq

.PHONY: compile clean distclean xref dialyze lint test test_compile

all: compile

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

distclean:
	rm -rf _build

xref:
	@$(REBAR) xref

dialyze:
	@$(REBAR) dialyzer

lint:
	@$(REBAR) as lint lint

test:
	@$(REBAR) ct --cover
	@$(REBAR) cover --verbose

test_compile:
	@$(REBAR) as test compile
