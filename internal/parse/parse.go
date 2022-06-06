package parse

import (
	"bufio"
	"os"
	"strings"

	"github.com/awilliamson10/golink/internal/utils"
	parquet "github.com/kostya-sh/parquet-go/parquet"
)

func ReadHeader(file string, delimiter string) (header []string, err error) {
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	parquet.ReadFileMetaData(f)
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	header = strings.Split(scanner.Text(), delimiter)
	return
}

func ParquetHeader(file string) (header []string, err error) {
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	md, err := parquet.ReadFileMetaData(f)
	if err != nil {
		return
	}
	for _, c := range md.GetSchema() {
		if c.GetName() == "schema" {
			continue
		}
		header = append(header, c.GetName())
	}
	return
}

func CleanNames(names []string) (cleaned []string) {
	for _, h := range names {
		dash := strings.ReplaceAll(h, "-", "_")
		period := strings.ReplaceAll(dash, ".", "_")
		nl := strings.ReplaceAll(period, "\n", "")
		ws := strings.ReplaceAll(nl, " ", "")
		cleaned = append(cleaned, strings.ToUpper(ws))
	}
	return
}

func CleanName(name string) string {
	dash := strings.ReplaceAll(name, "-", "_")
	period := strings.ReplaceAll(dash, ".", "_")
	nl := strings.ReplaceAll(period, "\n", "")
	ws := strings.ReplaceAll(nl, " ", "")
	return strings.ToUpper(ws)
}

func ParseFlagCnames(args map[string]string, cnames []string) map[string]string {
	var cname_options = map[string]string{
		CleanName(args["nstudy"]):  "NSTUDY",
		CleanName(args["snp"]):     "SNP",
		CleanName(args["ncol"]):    "N",
		CleanName(args["ncascol"]): "N_CAS",
		CleanName(args["nconcol"]): "N_CON",
		CleanName(args["a1"]):      "A1",
		CleanName(args["a2"]):      "A2",
		CleanName(args["p"]):       "P",
		CleanName(args["frq"]):     "FRQ",
		CleanName(args["info"]):    "INFO",
	}
	if args["infolist"] != "" {
		for _, info := range strings.Split(args["infolist"], ",") {
			cname_options[CleanName(info)] = "INFO"
		}
	}
	if args["signedsumstats"] != "" {
		ss := strings.Split(args["signedsumstats"], ",")
		cname := CleanName(ss[0])
		cname_options["NULL_VALUE"] = ss[1]
		cname_options[cname] = "SIGNED_SUMSTAT"
	}
	delete(cname_options, "")
	return cname_options
}

func GetCnameMap(flag map[string]string, dnames map[string]string, ignore []string) map[string]string {
	cname_map := map[string]string{}
	for key, value := range flag {
		if !utils.InList(key, ignore) {
			cname_map[key] = value
		}
	}

	for key, value := range dnames {
		if !utils.InList(key, ignore) && !utils.InList(key, utils.GetKeys(flag)) {
			cname_map[key] = value
		}
	}
	return cname_map
}

func FilterP(p float64) bool {
	if (p >= 1) || (p < 0) {
		return true
	}
	return false
}

func FilterFRQ(frq float64, mafmin float64) bool {
	if (frq > 1) || (frq < 0) {
		return true
	}
	if frq < mafmin {
		return true
	}
	return false
}

func FilterINFO(info float64, infomin float64) bool {
	if (info > 2) || (info < 0) {
		return true
	}
	if info <= infomin {
		return true
	}
	return false
}

func FilterAllele(allele string) bool {
	if !utils.InList(allele, []string{"A", "C", "G", "T"}) {
		return true
	}
	return false
}
