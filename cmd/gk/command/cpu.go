package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/klauspost/cpuid"
)

type Cpu struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Cpu) Run(args []string) (exitCode int) {
	fmt.Println(cpuid.CPU.BrandName, "Family", cpuid.CPU.Family, "Model:", cpuid.CPU.Model)
	fmt.Println("PhysicalCores:", cpuid.CPU.PhysicalCores, "ThreadsPerCore:", cpuid.CPU.ThreadsPerCore, "LogicalCores:", cpuid.CPU.LogicalCores)
	fmt.Println("Cacheline bytes:", cpuid.CPU.CacheLine)
	fmt.Println("L1 Data Cache:", gofmt.ByteSize(cpuid.CPU.Cache.L1D), "Instruction Cache:", gofmt.ByteSize(cpuid.CPU.Cache.L1D))
	fmt.Println("L2 Cache:", gofmt.ByteSize(cpuid.CPU.Cache.L2))
	fmt.Println("L3 Cache:", gofmt.ByteSize(cpuid.CPU.Cache.L3))

	// Test if we have a specific feature:
	if cpuid.CPU.SSE() {
		fmt.Println("We have Streaming SIMD Extensions")
	}

	return
}

func (*Cpu) Synopsis() string {
	return "Detect information about the CPU"
}

func (this *Cpu) Help() string {
	help := fmt.Sprintf(`
Usage: %s cpu

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
