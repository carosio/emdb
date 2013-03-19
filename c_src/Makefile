PRIVDIR := ../priv

CC ?= gcc

RM ?= rm

CFLAGS ?= -O2

override CFLAGS += -std=gnu99  -Wall -Wextra -fpic -I. $(shell erl -noinput -eval 'io:format("-I~s/erts-~s/include", [code:root_dir(), erlang:system_info(version)]), halt(0).')

override LDFLAGS += -shared -fpic 
#-lsystemd-journal

all : 	$(PRIVDIR)/emdb_drv.so 
#$(PRIVDIR)/mdb.o $(PRIVDIR)/midl.o 


$(PRIVDIR)/emdb_drv.so : emdb_drv.o midl.o mdb.o
	mkdir -p $(PRIVDIR)
	$(CC) $(LDFLAGS) -o $@ $^

clean:
	$(RM) -f $(PRIVDIR)/*.so $(PRIVDIR)/*.o
