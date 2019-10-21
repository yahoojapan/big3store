# -*- Makefile -*-
#
#	big3store/HOWTO Makefile
# 
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
# 
SOURCE = load-triples.md configure-system.md install.md user-interface.md aws.md
TARGET = $(SOURCE:%.md=%.html)

all: $(TARGET)

.SUFFIXES: .html .md

%.html: %.md
	markdown_py -x extra $< > $@

clean:
	rm *~ $(TARGET)
