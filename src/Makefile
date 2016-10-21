# -*- Makefile -*-
#
#	big3store Makefile
# 
# 	Copyright (C) 2014-2016 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
# 
APP_NAME = b3s

MODS = benchmark b3s_state node_state triple_distributor query_tree	\
       join_query_node hj_query_node mj_query_node tp_query_node	\
       query_node db_interface db_writer string_id file_reader		\
       user_interface repeater stop_watch rbutil $(APP_NAME)

HIPEMODS = benchmark query_node db_interface string_id file_reader	\
           $(APP_NAME)

PRJPTH = $(abspath ../../)
EPGDIR = $(PRJPTH)/../pub/epgsql
HEAP   = +hms 20971520 +hmbs 20971520
HSTNAM = $(shell hostname -f)

PRJNAM = big3store
EBIN   = ../ebin
EDOC   = ../docs
ELOG   = ../elog
SNDNAM = b3ss01@$(HSTNAM)
CNDNAM = b3ss02@$(HSTNAM)
UNDNAM = b3ss03@$(HSTNAM)
CNTNAM = b3ss99@$(HSTNAM)
OBSNAM = observer@$(HSTNAM)
OBSN01 = observer_01@$(HSTNAM)
OBSN02 = observer_02@$(HSTNAM)
OBSN03 = observer_03@$(HSTNAM)
OBSN04 = observer_04@$(HSTNAM)
OBSN05 = observer_05@$(HSTNAM)
EPGBIN = $(EPGDIR)/ebin
EPATH  = -pa $(EBIN) -pa $(EPGBIN)
SASL   = -boot start_sasl -config elog
MNESIA = -mnesia dir bak
MNSLRG = -mnesia dc_dump_limit 40 \
	 -mnesia dump_log_write_threshold 50000
ERLCKE = $(shell cat $(HOME)/.erlang.cookie)
COOKIE = -setcookie $(ERLCKE)
ERL    = erl -noshell
ERLL   = erl $(EPATH) $(SASL)
ERLLMN = erl $(EPATH) $(SASL) $(MNESIA)
ERLLNS = erl $(EPATH) $(SASL) $(MNESIA) -noshell
ERLLML = erl $(EPATH) $(SASL) $(MNESIA) -noshell $(MNSLRG) $(HEAP)
ERLNS  = erl $(EPATH) -noshell
YMDHMS = $(shell date +%Y%m%d%H%M%S)

all: compile bak ygtsv app $(HTWHTM)

app: $(APP_NAME).app
	@cp -p $(APP_NAME).app $(EBIN)

local0: $(APP_NAME).app.local0
	@cp -p $(APP_NAME).app.local0 $(EBIN)/$(APP_NAME).app

local1: $(APP_NAME).app.local1
	@cp -p $(APP_NAME).app.local1 $(EBIN)/$(APP_NAME).app

local2: $(APP_NAME).app.local2
	@cp -p $(APP_NAME).app.local2 $(EBIN)/$(APP_NAME).app

compile: ebin $(MODS:%=$(EBIN)/%.beam) TAGS

$(MODS:%=$(EBIN)/%.beam): record.hrl

EDOPT1 = {dir, edoc}
EDOPT2 = {todo, true}
EDOPT3 = {private, true}
EDOPTA = [$(EDOPT1), $(EDOPT2), $(EDOPT3)]
docs: edoc
	@rm -f bak
	$(ERL) -run edoc_run application "'$(APP_NAME)'" '"."' "$(EDOPTA)"
	@make bak

ebin:
	@mkdir -p $(EBIN)

edoc:
	@mkdir -p $(EDOC)
	@ln -sf $(EDOC) edoc
	@ln -sf ../src/overview.edoc edoc

elog:
	@mkdir -p $(ELOG)
	@ln -sf $(ELOG) elog

.SUFFIXES: .erl .beam

$(EBIN)/%.beam: %.erl
	erlc +export_all -W0 -o '$(EBIN)/' $<

hipe: $(HIPEMODS:%=%.erl)
	erlc +export_all +debug_info +native -W0 -o '$(EBIN)/' $(HIPEMODS:%=%.erl)

TAGS: $(MODS:%=%.erl)
	etags $(MODS:%=%.erl)

start: compile elog
	@echo \'make start-fs\' for starting a front server
	@echo \'make start-ds\' for starting data servers
	@echo \'make bootstrap\' for generating processes

SFSO01  = -name $(SNDNAM) -run b3s invoke  -b3s_mode front_server
SFSOPT  = $(SFSO01) -data_server_nodes $(DS)
SFSHLP  = DS="\"\\\"[{<node1>, <col1>}, {<node2>, <col2>}, ...].\\\"\""
start-fs: compile elog
	$(ERLLML) $(SFSO01) &

SDSOPT  = -name $(CNDNAM) -run b3s invoke -b3s_mode data_server
SDSOFS  = -front_server_node
SDSOCI  = -column_id 
start-ds: compile elog
	$(ERLLML) $(SDSOPT) &

BFSOPT  = -name $(CNTNAM) -run b3s bootstrap_command
bootstrap: compile elog
	$(ERLLNS) $(BFSOPT) &

term:	terminate-fs terminate-ds
	@echo terminating front and data servers...

TFSOPT  = -name $(CNTNAM) -run b3s terminate -node_name $(SNDNAM)
terminate-fs: compile elog
	$(ERLLNS) $(TFSOPT)

TDSOPT  = -name $(CNTNAM) -run b3s terminate -node_name $(CNDNAM)
terminate-ds: compile elog
	$(ERLLNS) $(TDSOPT)

prep-client: compile elog bak
	make term
	sleep 1
	make start-ds
	sleep 1

IFSOPT = -name $(CNTNAM) -run b3s investigate_files
investigate-files: compile elog
	$(ERLLNS) $(IFSOPT)

LFSOPT = -name $(CNTNAM) -run b3s load_files
load-files: compile elog
	$(ERLLNS) $(LFSOPT)

UIMOPT = -name $(UNDNAM) -run user_interface main
ui-main: compile elog
ifndef A
	$(ERLNS) $(UIMOPT)
else
	$(ERLNS) $(UIMOPT) -node_name $(A)
endif

TSLOG = test-$(YMDHMS).log
TALOG = test-all-$(YMDHMS).log
TSOPT = -name $(SNDNAM) -front_server_node $(SNDNAM) -run rbutil	\
	test_halt
SEP55 = "======================================================="
TSTPS = "tests passed"

test-all:
	make test-all-perform
	make test-all-grep

test-all-perform: t0s1 t0s2 t1s1 t1s2 t2s1 t2td t2bm

test-all-grep:
	@grep -i "*failed*" *.log | tee -a $(TALOG)
	@echo $(SEP55) | tee -a $(TALOG)
	@grep $(TSTPS) *.log | tee -a $(TALOG)

# TSMDSKN = "[b3s]."
# TSMDSKN = "[b3s_state]."
# TSMDSKN = "[node_state]." # for initialize b3s_state
# TSMDSKN = "[file_reader]."
# TSMDSKN = "[tp_query_node]."
# TSMDSKN = "[join_query_node]."
# TSMDSKN = "[hj_query_node]."
# TSMDSKN = "[mj_query_node]."
TSMDSKN = "[db_interface]."
# TSMDSKN = "[db_writer]."
# TSMDSKN = "[query_tree]."
# TSMDSKN = "[string_id]."
# TSMDSKN = "[user_interface]."
# TSMDSKN = "[query_node]."
TSMDSKNS1 = "[b3s, b3s_state, node_state, file_reader, db_interface, db_writer, string_id, user_interface, query_node]." # step 1
TSMDSKNS2 = "[tp_query_node, join_query_node, hj_query_node, mj_query_node, query_tree]." # step 2
TSMDSKNTD = "[triple_distributor]."
TSMDSKNBM = "[benchmark]."

t0: compile elog bak local0 term
	$(ERLLNS) $(TSOPT) $(TSMDSKN) | tee $(TSLOG)

t1: compile elog bak local1 term
	$(ERLLNS) $(TSOPT) $(TSMDSKN) | tee $(TSLOG)

t2: compile elog bak local2 term
	$(ERLLNS) $(TSOPT) $(TSMDSKN) | tee $(TSLOG)

t0s1: compile elog bak local0 term
	$(ERLLNS) $(TSOPT) $(TSMDSKNS1) | tee $(TSLOG)

t0s2: compile elog bak local0 term
	$(ERLLNS) $(TSOPT) $(TSMDSKNS2) | tee $(TSLOG)

t1s1: compile elog bak local1 term
	$(ERLLNS) $(TSOPT) $(TSMDSKNS1) | tee $(TSLOG)

t1s2: compile elog bak local1 term
	$(ERLLNS) $(TSOPT) $(TSMDSKNS2) | tee $(TSLOG)

t2s1: compile elog bak local2 prep-client
	$(ERLLNS) $(TSOPT) $(TSMDSKNS1) | tee $(TSLOG)

t2td: compile elog bak local2 prep-client
	$(ERLLNS) $(TSOPT) $(TSMDSKNTD) | tee $(TSLOG)

t2bm: compile elog bak local2 prep-client
	$(ERLLNS) $(TSOPT) $(TSMDSKNBM) | tee $(TSLOG)

SMPLOG = sample_log
HSTNMS = $(shell hostname -s)
LSALOG = $(SMPLOG)/$(HSTNMS)-$(YMDHMS).log

l: elog
	$(ERLL) -run rb start

lg: elog compile
ifndef A
	@echo ex. make lg A=grep_string
else
	$(ERLLNS) -run rbutil rb_grep $(A)
endif

lfd: elog compile
ifndef A
	@echo ex. make lfd A=days
else
	$(ERLLNS) -run rbutil rb_filter_by_date $(A) d
endif

lfh: elog compile
ifndef A
	@echo ex. make lfh A=hours
else
	$(ERLLNS) -run rbutil rb_filter_by_date $(A) h
endif

lfm: elog compile
ifndef A
	@echo ex. make lfm A=minutes
else
	$(ERLLNS) -run rbutil rb_filter_by_date $(A) m
endif

lfml:
	mkdir -p $(SMPLOG)
	make lfm A=$(A)  > $(LSALOG)
	ls -lt $(SMPLOG)/ | head

lfs: elog compile
ifndef A
	@echo ex. make lfs A=seconds
else
	$(ERLLNS) -run rbutil rb_filter_by_date $(A) s
endif

lla: elog compile
	$(ERLLNS) -run rbutil rb_list

llc: elog compile
	$(ERLLNS) -run rbutil rb_list crash_report

lls: elog compile
	$(ERLLNS) -run rbutil rb_list supervisor_report

lle: elog compile
	$(ERLLNS) -run rbutil rb_list error

llp: elog compile
	$(ERLLNS) -run rbutil rb_list progress

llir: elog compile
	$(ERLLNS) -run rbutil rb_list info_report

llim: elog compile
	$(ERLLNS) -run rbutil rb_list info_msg

lsa: elog compile
	$(ERLLNS) -run rbutil rb_show all

lsal: elog compile
	mkdir -p $(SMPLOG)
	$(ERLLNS) -run rbutil rb_show all > $(LSALOG)
	ls -lt $(SMPLOG)/ | head

lsc: elog compile
	$(ERLLNS) -run rbutil rb_show crash_report

lss: elog compile
	$(ERLLNS) -run rbutil rb_show supervisor_report

lse: elog compile
	$(ERLLNS) -run rbutil rb_show error

lsp: elog compile
	$(ERLLNS) -run rbutil rb_show progress

lsir: elog compile
	$(ERLLNS) -run rbutil rb_show info_report

lsim: elog compile
	$(ERLLNS) -run rbutil rb_show info_msg

es: elog
	$(ERLLMN) -name $(SNDNAM)

ec: elog
	$(ERLLMN) -name $(CNDNAM)

ee: elog
	$(ERLLMN) -name b3ss98@$(HSTNAM)

e: elog all
	$(ERLLMN) -name $(CNTNAM)

debug: elog compile
	$(ERLL) -run debugger start

PSCMD  = ps auxw | grep beam | grep b3ss | grep -v grep
ps:
	@ps auxw | head -1
	@$(PSCMD)

observer:
	erl -name $(OBSNAM) -hidden -run observer

obs1:
	erl -name $(OBSN01) -hidden -run observer

obs2:
	erl -name $(OBSN02) -hidden -run observer

obs3:
	erl -name $(OBSN03) -hidden -run observer

obs4:
	erl -name $(OBSN04) -hidden -run observer

obs5:
	erl -name $(OBSN05) -hidden -run observer

CRDVSS = -noinput -s crashdump_viewer script_start 
ERLCRD = erl_crash.dump
cdv:
ifndef A
	erl -sname cdv $(CRDVSS) $(ERLCRD)
else
	erl -sname cdv $(CRDVSS) $(A)
endif

BAKDIR = $(PRJPTH)/bak

bak:
	mkdir -p $(BAKDIR)
	ln -s $(BAKDIR)

rebar:
	ln -s $(PRJPTH)/rebar/rebar

clean: bak logclean
	rm -fr $(EBIN) ../erl_crash.dump bak $(BDBDRV) $(BDBNIF)
	-mv -f *~ *.beam edoc elog erl_crash.dump TAGS *.log sr0*.png $(BAKDIR)

logclean:
	rm -fr $(ELOG) test-*.log

# ====> END OF LINE <====
