/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"github.com/awilliamson10/golink/scripts"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// mungeSumstatsCmd represents the mungeSumstats command
var mungeSumstatsCmd = &cobra.Command{
	Use:   "munge-sumstats",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		opts := make(map[string]string)
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			opts[f.Name] = f.Value.String()
		})
		scripts.Munge_sumstats(opts)
	},
	FParseErrWhitelist: cobra.FParseErrWhitelist{
		UnknownFlags: true,
	},
}

var (
	sumstats      string
	signedsumstat string
	ncol          string
	nstudy        string
	snp           string
	ncascol       string
	nconcol       string
	a1            string
	a2            string
	p             string
	frq           string
	info          string
	infolist      string
	a1inc         string
	ignore        string
	mafmin        string
)

func init() {
	runCmd.AddCommand(mungeSumstatsCmd)

	mungeSumstatsCmd.Flags().StringVarP(&sumstats, "sumstats", "s", "", "Sumstats file")
	mungeSumstatsCmd.Flags().StringVarP(&signedsumstat, "signedsumstat", "S", "", "Signed sumstat file")
	mungeSumstatsCmd.Flags().StringVarP(&ncol, "ncol", "n", "N", "Ncol")
	mungeSumstatsCmd.Flags().StringVarP(&nstudy, "nstudy", "N", "", "Nstudy")
	mungeSumstatsCmd.Flags().StringVarP(&snp, "snp", "p", "SNP", "Snp")
	mungeSumstatsCmd.Flags().StringVarP(&ncascol, "ncascol", "c", "Ncas", "Ncascol")
	mungeSumstatsCmd.Flags().StringVarP(&nconcol, "nconcol", "C", "Ncon", "Nconcol")
	mungeSumstatsCmd.Flags().StringVarP(&a1, "a1", "a", "A1", "A1")
	mungeSumstatsCmd.Flags().StringVarP(&a2, "a2", "b", "A2", "A2")
	mungeSumstatsCmd.Flags().StringVarP(&p, "p", "P", "P", "P")
	mungeSumstatsCmd.Flags().StringVarP(&frq, "frq", "F", "FRQ", "Frq")
	mungeSumstatsCmd.Flags().StringVarP(&info, "info", "I", "INFO", "Info")
	mungeSumstatsCmd.Flags().StringVarP(&infolist, "infolist", "i", "", "Info list")
	mungeSumstatsCmd.Flags().StringVarP(&a1inc, "a1inc", "A", "false", "A1inc")
	mungeSumstatsCmd.Flags().StringVarP(&ignore, "ignore", "", "", "Ignore")
	mungeSumstatsCmd.Flags().StringVarP(&mafmin, "mafmin", "M", "0.0", "Mafmin")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mungeSumstatsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mungeSumstatsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
