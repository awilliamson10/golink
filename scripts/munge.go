package scripts

import (
	"io"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/awilliamson10/golink/internal/constants"
	"github.com/awilliamson10/golink/internal/ops"
	parse "github.com/awilliamson10/golink/internal/parse"
	"github.com/awilliamson10/golink/internal/utils"
)

func Munge_sumstats(args map[string]string) {
	// setup logger to out.log
	// open out + ".log" for writing
	out := args["out"]
	logFile, err := os.OpenFile(out+".log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log := log.New(mw, "", log.LstdFlags)

	log.Printf("Munging sumstats of %s\n", args["sumstats"])

	sumstats := args["sumstats"]
	file_cnames, err := parse.ReadHeader(sumstats, "\t")
	if err != nil {
		log.Printf("Error reading header: %s\n", err)
		return
	}

	cleaned_cnames := parse.CleanNames(file_cnames)
	flag_cnames := parse.ParseFlagCnames(args, cleaned_cnames)

	ignore_cnames := []string{}
	if args["ignore"] != "" {
		ignore_list := strings.Split(args["ignore"], ",")
		for _, ignore := range ignore_list {
			ignore_cnames = append(ignore_cnames, parse.CleanName(ignore))
		}
	}

	mod_default_cnames := map[string]string{}
	if args["signedsumstats"] != "" || args["a1inc"] != "false" {
		for key, value := range constants.Default_cnames {
			if !utils.InList(value, utils.GetKeys(constants.Null_values)) {
				mod_default_cnames[key] = value
			}
		}
	} else {
		mod_default_cnames = constants.Default_cnames
	}

	cname_map := parse.GetCnameMap(flag_cnames, mod_default_cnames, ignore_cnames)

	cname_translation := map[string]string{}
	for _, value := range cleaned_cnames {
		if utils.InList(value, utils.GetKeys(cname_map)) {
			cname_translation[value] = cname_map[value]
		}
	}

	cname_description := map[string]string{}
	for key, value := range cname_translation {
		cname_description[key] = constants.Describe_cname[value]
	}

	//sign_cname := "SIGNED_SUMSTATS"
	if args["signedsumstats"] != "" && args["a1inc"] != "false" {
		sign_cnames := []string{}
		for key := range cname_translation {
			if utils.InList(key, utils.GetKeys(constants.Null_values)) {
				sign_cnames = append(sign_cnames, key)
			}
		}
		if len(sign_cnames) == 0 || len(sign_cnames) > 1 {
			log.Fatal("Error: --signed_sumstats must be followed by a single column name")
		}
		sign_cname := sign_cnames[0]
		//signed_sumstat_null := null_values[cname_translation[sign_cname]]
		cname_translation[sign_cname] = "SIGNED_SUMSTAT"
	}

	// Check that we have all the required columns
	req_cols := []string{"SNP", "P"}
	if args["a1inc"] != "false" {
		req_cols = append(req_cols, "SIGNED_SUMSTAT")
	}

	for _, col := range req_cols {
		if !utils.InList(col, utils.GetValues(cname_translation)) {
			log.Fatal("Error: missing required column: " + col)
		}
	}

	for key, value := range cname_translation {
		num_occ := utils.CountOccurrences(key, cleaned_cnames)
		if num_occ > 1 {
			log.Fatal("Error: column name " + key + " occurs more than once")
		}
		num_occ_v := utils.CountOccurrences(value, utils.GetValues(cname_translation))
		if num_occ_v > 1 {
			log.Fatal("Error: column name " + value + " occurs more than once")
		}
	}

	// Check that there is an N column
	if (args["n"] == "0") && (args["ncas"] == "0" && args["ncon"] == "0") &&
		!utils.InList("N", utils.GetValues(cname_translation)) &&
		(!utils.InList("N_CAS", utils.GetValues(cname_translation)) || !utils.InList("N_CON", utils.GetValues(cname_translation))) {
		log.Fatal("Error: Could not determine N.")
	}

	if (utils.InList("N", utils.GetValues(cname_translation)) || utils.InList("N_CAS", utils.GetValues(cname_translation)) && utils.InList("N_CON", utils.GetValues(cname_translation))) &&
		utils.InList("NSTUDY", utils.GetValues(cname_translation)) {
		nstudy := utils.Filter(utils.GetValues(cname_translation), func(s string) bool {
			return s == "NSTUDY"
		})
		for _, x := range nstudy {
			delete(cname_translation, x)
		}
	}
	if args["noalleles"] != "false" && !utils.InList("A1", utils.GetValues(cname_translation)) && !utils.InList("A2", utils.GetValues(cname_translation)) {
		log.Fatal("Error: Could not find A1/A2 columns.")
	}

	log.Println("Interpreting column names.")
	for key, value := range cname_description {
		log.Println(key + ": " + value)
	}

	/* 	var merged_alleles df.parse
	   	if args["merge_alleles"] != "" {
	   		log.Println("Reading list of SNPs for allele merge from " + args["merge_alleles"])
	   		merge_alleles := ReadCSV(args["merge_alleles"], '\t', []string{}, []string{"."}, map[string]series.Type{})

	   		for _, col := range []string{"SNP", "A1", "A2"} {
	   			if !utils.InList(col, merge_alleles.Names()) {
	   				log.Fatal("Error: missing required column: " + col)
	   			}
	   		}
	   		nrows, _ := merge_alleles.Dims()
	   		log.Printf("Read %d SNPs for allele merge.", nrows)

	   		merge_alleles_combined := merge_alleles.Mutate(
	   			series.New(AppendSeries(merge_alleles.Col("A1").Records(), merge_alleles.Col("A2").Records()), series.String, "MA"),
	   		)

	   		merged_alleles := merge_alleles_combined.Select([]string{"SNP", "MA"})
	   	} */

	// Read the data
	log.Println("Reading data.")
	ctypes := map[string]arrow.DataType{}
	for _, value := range cleaned_cnames {
		if utils.InList(value, constants.Numeric_cols) {
			ctypes[value] = arrow.PrimitiveTypes.Float64
		} else {
			ctypes[value] = arrow.BinaryTypes.String
		}
	}

	data, schema := ops.ArrowCSV(args["sumstats"], cleaned_cnames, '\t', ctypes)
	log.Println("Read", data.NumRows(), "rows.")

	parsed, schema := ops.ParseDataframe(data, schema, cname_translation)
	log.Println("Parsed", parsed.NumRows(), "rows.")

	ops.ParseDataframe(data, schema, cname_translation)
	ops.ParseDataframe(data, schema, cname_translation)

	//df := ops.RemoveDuplicateSNPS(parsed, schema)
	//log.Println("Left with", df.NumRows(), "SNPs.")

	//ops.ProcessN(df, schema, []string{args["ncol"], args["ncas"], args["ncon"]})

}
