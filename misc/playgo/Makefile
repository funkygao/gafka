all:ast

sym:
	go tool compile -E foo.go

lex:
	go tool compile -x foo.go

ast:
	go tool compile -W foo.go

asm:
	go tool compile -S foo.go

compile:foo.go
	go tool compile foo.go

l:compile
	go tool link -n -v -dumpdep foo.o

help:
	go tool compile
