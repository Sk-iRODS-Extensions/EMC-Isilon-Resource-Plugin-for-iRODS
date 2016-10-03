SUBS = 	isilon
export BUILD_FLAGS =

######################################################################
# Configuration should occur above this line

.PHONY: ${SUBS} clean

default: BUILD_FLAGS += -s
default: ${SUBS}

debug: BUILD_FLAGS += -DISILON_DEBUG -DISILON_DUMP_THR_ID -g
debug: ${SUBS}

${SUBS}:
	${MAKE} -C $@

clean:
	@-for dir in ${SUBS}; do \
	${MAKE} -C $$dir clean; \
	done
