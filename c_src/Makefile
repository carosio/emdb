PRIVDIR := ../priv

CC ?= gcc

RM ?= rm

CFLAGS ?= -O2

override CFLAGS += -std=gnu99  -Wall -fpic -I. $(shell erl -noinput -eval 'io:format("-I~s/erts-~s/include", [code:root_dir(), erlang:system_info(version)]), halt(0).')

override LDFLAGS += -shared -fpic 
#-lsystemd-journal

all : 	$(PRIVDIR)/mdb.so $(PRIVDIR)/midl.so $(PRIVDIR)/emdb_drv.so

$(PRIVDIR)/%.so : %.o
	mkdir -p $(PRIVDIR)
	$(CC) $(LDFLAGS) -o $@ $^

# $(PRIVDIR)/midl.so : midl.o
# 	mkdir -p $(PRIVDIR)
# 	$(CC) $(LDFLAGS) -o $@ $^

# $(PRIVDIR)/emd_drv.so : emdb_drv.o
# 	mkdir -p $(PRIVDIR)
# 	$(CC) $(LDFLAGS) -o $@ $^

clean:
	$(RM) -f $(PRIVDIR)/*.so $(PRIVDIR)/*.o
