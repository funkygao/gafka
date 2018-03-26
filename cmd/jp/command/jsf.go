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

type Jsf struct {
	Ui  cli.Ui
	Cmd string

	*java.BaseJavaParserListener // https://godoc.org/bramp.net/antlr4/java#BaseJavaParserListener

	packageName, interfaceName, annotationName string
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
			// all dubbo services reside in *Service.java
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
		//parser.AddErrorListener(antlr.NewDiagnosticErrorListener(true)) TODO
		// walk the tree
		antlr.ParseTreeWalkerDefault.Walk(this, parser.CompilationUnit())
		return nil
	}))
}

func (this *Jsf) EnterPackageDeclaration(ctx *java.PackageDeclarationContext) {
	this.packageName = ctx.GetText()[len("package") : len(ctx.GetText())-1]
}

func (this *Jsf) EnterInterfaceDeclaration(ctx *java.InterfaceDeclarationContext) {
	this.interfaceName = ctx.GetTokens(java.JavaLexerIDENTIFIER)[0].GetText()
}

func (this *Jsf) EnterInterfaceMethodDeclaration(ctx *java.InterfaceMethodDeclarationContext) {
	methodName := ctx.GetTokens(java.JavaLexerIDENTIFIER)[0]
	if strings.HasPrefix(methodName.GetText(), "echo") {
		// ignore health check method
		return
	}

	this.Ui.Outputf("%10s %s.%s.%s", "Jsf", this.packageName, this.interfaceName, methodName)
}

func (this *Jsf) EnterAnnotation(ctx *java.AnnotationContext) {
	this.annotationName = ctx.GetText()
}

func (this *Jsf) ExitAnnotation(ctx *java.AnnotationContext) {
}

func (this *Jsf) EnterCompilationUnit(ctx *java.CompilationUnitContext) {

}

func (this *Jsf) EnterConstDeclaration(ctx *java.ConstDeclarationContext) {

}

func (this *Jsf) EnterDefaultValue(ctx *java.DefaultValueContext) {

}

func (this *Jsf) EnterElementValue(ctx *java.ElementValueContext) {

}

func (this *Jsf) EnterElementValuePair(ctx *java.ElementValuePairContext) {

}

func (this *Jsf) EnterFieldDeclaration(ctx *java.FieldDeclarationContext) {

}

func (this *Jsf) EnterClassDeclaration(ctx *java.ClassDeclarationContext) {

}

func (this *Jsf) EnterArguments(ctx *java.ArgumentsContext) {

}

func (this *Jsf) EnterBlock(ctx *java.BlockContext) {

}

func (this *Jsf) EnterMethodDeclaration(ctx *java.MethodDeclarationContext) {

}

func (this *Jsf) EnterBlockStatement(ctx *java.BlockStatementContext) {

}

func (this *Jsf) EnterCatchClause(ctx *java.CatchClauseContext) {

}

func (this *Jsf) EnterFinallyBlock(ctx *java.FinallyBlockContext) {

}

func (*Jsf) Synopsis() string {
	return "List JSF provider services from java files"
}

func (this *Jsf) Help() string {
	help := fmt.Sprintf(`
Usage: %s jsf path

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
