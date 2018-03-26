package command

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"bramp.net/antlr4/java" // Precompiled Go versions of Java grammar
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/funkygao/gocli"
)

//   go:generate java -jar /Users/funky/bin/antlr-4.7-complete.jar -Dlanguage=Go -o parser template/Java.g4

type Jsf struct {
	Ui  cli.Ui
	Cmd string

	*java.BaseJavaParserListener // https://godoc.org/bramp.net/antlr4/java#BaseJavaParserListener
}

func (this *Jsf) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jsf", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.scanJsfServices(args[len(args)-1])

	return
}

func (this *Jsf) scanJsfServices(root string) {
	swallow(filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(f.Name()), "service.java") {
			return nil
		}

		// https://blog.gopheracademy.com/advent-2017/parsing-with-antlr4-and-go/
		// https://github.com/bramp/antlr4-grammars
		is, e := antlr.NewFileStream(path)
		swallow(e)
		// create the lexer
		lexer := java.NewJavaLexer(is)
		stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		// create the parser
		parser := java.NewJavaParser(stream)
		parser.BuildParseTrees = true
		//parser.AddErrorListener(antlr.NewDiagnosticErrorListener(true))
		// walk the tree
		antlr.ParseTreeWalkerDefault.Walk(this, parser.CompilationUnit())

		return nil
	}))
}

func (this *Jsf) EnterAnnotation(ctx *java.AnnotationContext) {
	this.Ui.Outputf("annotation %s", ctx.ElementValue().GetText())
}

func (this *Jsf) EnterInterfaceMethodDeclaration(ctx *java.InterfaceMethodDeclarationContext) {
	this.Ui.Outputf("interface %s", ctx.GetText())
}

func (*Jsf) Synopsis() string {
	return "Extract JSF provider services from java files"
}

func (this *Jsf) Help() string {
	help := fmt.Sprintf(`
Usage: %s jsf path

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
