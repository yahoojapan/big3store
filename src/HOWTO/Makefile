SOURCE = load-triples.md configure-system.md install.md user-interface.md
TARGET = $(SOURCE:%.md=%.html)

all: $(TARGET)

.SUFFIXES: .html .md

%.html: %.md
	markdown_py -x extra $< > $@

clean:
	rm *~ $(TARGET)
