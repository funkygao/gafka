package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"bramp.net/antlr4/java" // Precompiled Go versions of Java grammar
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/funkygao/columnize"
	"github.com/funkygao/gocli"
	"github.com/pmylund/sortutil"
)

type Provider struct {
	Ui  cli.Ui
	Cmd string

	compactMode bool
	longFormat  bool
	dumpMode    bool
	maxDumped   int

	*java.BaseJavaParserListener // https://godoc.org/bramp.net/antlr4/java#BaseJavaParserListener

	packageName, interfaceName, annotationName string

	interfaces []string
	providers  map[string]*jsfProvider
}

type jsfProvider struct {
	name    string
	methodN int
}

func (this *Provider) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("provider", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&this.compactMode, "c", false, "")
	cmdFlags.BoolVar(&this.longFormat, "l", false, "")
	cmdFlags.BoolVar(&this.dumpMode, "d", true, "")
	cmdFlags.IntVar(&this.maxDumped, "max", 10, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.interfaces = make([]string, 0, 100)
	this.providers = make(map[string]*jsfProvider, 10)
	this.scanProviderServices(args[len(args)-1])

	// report the parsing phase
	if this.compactMode {
		this.Ui.Output(strings.Join(this.interfaces, ","))
		return
	}

	// display the summary
	summary := make([]jsfProvider, 0, len(this.providers))
	for _, p := range this.providers {
		summary = append(summary, *p)
	}
	sortutil.AscByField(summary, "methodN")

	interfaceN, methodN := 0, 0
	lines := []string{"Interface|Methods"}
	for _, p := range summary {
		lines = append(lines, fmt.Sprintf("%s|%d", p.name, p.methodN))
		interfaceN++
		methodN += p.methodN
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output(strings.Repeat("-", 80))
	this.Ui.Outputf("Interface:%d Method:%d", interfaceN, methodN)

	if this.dumpMode {
		interfaces := make([]string, 0, this.maxDumped)
		sortutil.DescByField(summary, "methodN")
		for i, p := range summary {
			if i >= this.maxDumped {
				break
			}

			interfaces = append(interfaces, p.name)
		}

		ioutil.WriteFile(jsfFile(), []byte(strings.Join(interfaces, ",")), 0600)
	}

	return
}

func (this *Provider) scanProviderServices(root string) {
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

func (this *Provider) EnterPackageDeclaration(ctx *java.PackageDeclarationContext) {
	this.packageName = ctx.GetText()[len("package") : len(ctx.GetText())-1]
}

func (this *Provider) EnterInterfaceDeclaration(ctx *java.InterfaceDeclarationContext) {
	this.interfaceName = ctx.GetTokens(java.JavaLexerIDENTIFIER)[0].GetText()
	this.interfaces = append(this.interfaces, fmt.Sprintf("%s.%s", this.packageName, this.interfaceName))
}

func (this *Provider) EnterInterfaceMethodDeclaration(ctx *java.InterfaceMethodDeclarationContext) {
	methodName := ctx.GetTokens(java.JavaLexerIDENTIFIER)[0]
	if strings.HasPrefix(methodName.GetText(), "echo") {
		// ignore health check method
		this.annotationName = ""
		return
	}

	fullInterfaceName := fmt.Sprintf("%s.%s", this.packageName, this.interfaceName)
	if _, present := this.providers[fullInterfaceName]; present {
		this.providers[fullInterfaceName].methodN++
	} else {

		this.providers[fullInterfaceName] = &jsfProvider{name: fullInterfaceName, methodN: 1}
	}

	if this.longFormat {
		this.Ui.Outputf("%80s %s", fullInterfaceName, methodName)
	}

	this.annotationName = ""
}

func (this *Provider) EnterAnnotation(ctx *java.AnnotationContext) {
	this.annotationName = ctx.GetText()
}

func (this *Provider) ExitAnnotation(ctx *java.AnnotationContext) {
}

func (this *Provider) EnterCompilationUnit(ctx *java.CompilationUnitContext) {

}

func (this *Provider) EnterConstDeclaration(ctx *java.ConstDeclarationContext) {

}

func (this *Provider) EnterDefaultValue(ctx *java.DefaultValueContext) {

}

func (this *Provider) EnterElementValue(ctx *java.ElementValueContext) {

}

func (this *Provider) EnterElementValuePair(ctx *java.ElementValuePairContext) {

}

func (this *Provider) EnterFieldDeclaration(ctx *java.FieldDeclarationContext) {

}

func (this *Provider) EnterClassDeclaration(ctx *java.ClassDeclarationContext) {

}

func (this *Provider) EnterArguments(ctx *java.ArgumentsContext) {

}

func (this *Provider) EnterBlock(ctx *java.BlockContext) {

}

func (this *Provider) EnterMethodDeclaration(ctx *java.MethodDeclarationContext) {

}

func (this *Provider) EnterBlockStatement(ctx *java.BlockStatementContext) {

}

func (this *Provider) EnterCatchClause(ctx *java.CatchClauseContext) {

}

func (this *Provider) EnterFinallyBlock(ctx *java.FinallyBlockContext) {

}

func (*Provider) Synopsis() string {
	return "List JSF provider services from java files"
}

func (this *Provider) Help() string {
	help := fmt.Sprintf(`
Usage: %s provider path

    %s

Options:

    -c
      Compact mode. 
      Generate interface names seperated by comma.

    -d
      Dump interfaces to %s.
      Default is true.

    -max N
      How many interfaces are dumped at most.
      Default is 10.

    -l
      Use a long listing format.

`, this.Cmd, this.Synopsis(), jsfFile())
	return strings.TrimSpace(help)
}
