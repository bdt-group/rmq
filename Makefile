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

dc_%: compose
		$(make $*)

compose: export DC_UID   = $(shell id -u)
compose: export DC_GID   = $(shell id -g)
compose: export DC_USER  = $(shell id -un)
compose: export DC_GROUP = $(shell id -gn)
compose:
	docker-compose up -d

decompose: export DC_UID   = $(shell id -u)
decompose: export DC_GID   = $(shell id -g)
decompose: export DC_USER  = $(shell id -un)
decompose: export DC_GROUP = $(shell id -gn)
decompose:
	docker-compose down
