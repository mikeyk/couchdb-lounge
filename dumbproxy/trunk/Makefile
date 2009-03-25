all: dumbproxy

clean:
	$(MAKE) -C nginx clean

install:
	$(MAKE) -C nginx install

dumbproxy:
	$(MAKE) -C nginx

tags:
	ctags -R *
	cscope -Rb
