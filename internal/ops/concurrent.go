package ops

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/awilliamson10/golink/internal/constants"
	"github.com/awilliamson10/golink/internal/parse"
	"github.com/awilliamson10/golink/internal/utils"
	"gonum.org/v1/gonum/floats"
)

func ArrowCSV(file string, header []string, delimiter rune, ctypes map[string]arrow.DataType) (table array.Table, schema *arrow.Schema) {
	defer utils.TimeTrack(time.Now(), "ArrowCSV")

	fields := make([]arrow.Field, 0)
	for _, c := range header {
		field := arrow.Field{Name: c, Type: ctypes[c], Nullable: true, Metadata: arrow.Metadata{}}
		fields = append(fields, field)
	}
	schema = arrow.NewSchema(fields, nil)

	rFile, err := os.Open(file) //3 columns
	if err != nil {
		log.Println("Error:", err)
		return
	}
	defer rFile.Close()

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	r := csv.NewReader(
		rFile,
		schema,
		csv.WithHeader(true),
		csv.WithAllocator(mem),
		csv.WithChunk(200),
		csv.WithComma('\t'),
	)
	defer r.Release()

	records := make([]array.Record, 0)
	for r.Next() {
		rec := r.Record()
		rec.Retain()
		records = append(records, rec)
	}
	log.Println("Finished Reading.")

	table = array.NewTableFromRecords(schema, records)
	return
}

func ParseDataframe(table array.Table, schema *arrow.Schema, cnames map[string]string) (new_table array.Table, new_schema *arrow.Schema) {
	defer utils.TimeTrack(time.Now(), "ParseDataframe")
	log.Println("Parsing dataframe.")

	fields := make([]arrow.Field, 0)
	for _, c := range schema.Fields() {
		if utils.InList(c.Name, utils.GetKeys(cnames)) {
			switch c.Type.ID().String() {
			case "FLOAT64":
				c = arrow.Field{Name: cnames[c.Name], Type: arrow.PrimitiveTypes.Float64, Nullable: true, Metadata: arrow.Metadata{}}
				break
			case "STRING":
				c = arrow.Field{Name: cnames[c.Name], Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.Metadata{}}
				break
			}
			fields = append(fields, c)
		}
	}
	new_schema = arrow.NewSchema(fields, nil)
	if new_schema.Equal(schema) {
		log.Println("Schema is the same.")
		new_schema = schema
	}

	tr := array.NewTableReader(table, 200)
	dropped := map[string]int{}
	records := make([]array.Record, 0)
	for tr.Next() {
		rec := tr.Record()
		rec.Retain()
		drop_idxs := []int{}
		for i, col := range rec.Columns() {
			colname := rec.ColumnName(i)
			if utils.InList(colname, constants.Numeric_cols) {
				d := array.NewFloat64Data(col.Data())
				for i, v := range d.Float64Values() {
					switch colname {
					case "P":
						if parse.FilterP(v) {
							dropped["P"]++
							drop_idxs = append(drop_idxs, i)
						}
						break
					case "FRQ":
						if parse.FilterFRQ(v, 0.05) {
							dropped["FRQ"]++
							drop_idxs = append(drop_idxs, i)
						}
						break
					case "INFO":
					case "INFO_LIST":
						if parse.FilterINFO(v, 0.05) {
							dropped["INFO"]++
							drop_idxs = append(drop_idxs, i)
						}
						break
					}
				}
			} else {
				d := array.NewStringData(col.Data())
				for i := 0; i < d.Len(); i++ {
					switch colname {
					case "A1":
					case "A2":
						if parse.FilterAllele(strings.ToUpper(d.Value(i))) {
							dropped["A"]++
							drop_idxs = append(drop_idxs, i)
						}
					}
				}
			}
		}
		if len(drop_idxs) > 0 {
			slice_idxs := utils.Slices(int(rec.NumRows()), drop_idxs)
			drop_idxs = []int{}
			new_cols := make([]array.Interface, 0)
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			for i, col := range rec.Columns() {
				if !utils.InList(rec.ColumnName(i), utils.GetKeys(cnames)) {
					continue
				}
				new_data := make([]array.Interface, 0)
				for _, idx := range slice_idxs {
					ns := array.NewSliceData(col.Data(), int64(idx[0]), int64(idx[1]))
					ns.Retain()
					if col.DataType().ID() == arrow.FLOAT64 {
						//log.Println(rec.ColumnName(i), " : ", col.DataType().ID().String())
						new_data = append(new_data, array.NewFloat64Data(ns))
					} else {
						//log.Println(rec.ColumnName(i), " : ", col.DataType().ID().String())
						new_data = append(new_data, array.NewStringData(ns))
					}
					ns.Release()
				}
				new_col, err := array.Concatenate(new_data, mem)
				if err != nil {
					log.Println("Error:", err)
					return
				}
				new_col.Retain()
				new_cols = append(new_cols, new_col)
				new_col.Release()
			}
			new_rec := array.NewRecord(new_schema, new_cols, int64(new_cols[0].Data().Len()))
			new_rec.Retain()
			records = append(records, new_rec)
			new_rec.Release()
		} else {
			new_cols := make([]array.Interface, 0)
			for i := range rec.Columns() {
				if !utils.InList(rec.ColumnName(i), utils.GetKeys(cnames)) {
					continue
				}
				new_cols = append(new_cols, rec.Column(i))
			}
			new_rec := array.NewRecord(new_schema, new_cols, int64(new_cols[0].Data().Len()))
			new_rec.Retain()
			records = append(records, new_rec)
			new_rec.Release()
		}
		rec.Release()
	}
	log.Println("Finished Parsing.")
	log.Println("Dropped:", dropped)
	new_table = array.NewTableFromRecords(new_schema, records)
	return
}

func RemoveDuplicateSNPS(table array.Table, schema *arrow.Schema) (new_table array.Table) {
	defer utils.TimeTrack(time.Now(), "RemoveDuplicateSNPS")
	log.Println("Removing duplicate SNPs.")

	tr := array.NewTableReader(table, 200)

	dropped := map[string]int{}
	SNPS := map[string]bool{}
	records := make([]array.Record, 0)
	for tr.Next() {
		rec := tr.Record()
		rec.Retain()
		drop_idxs := []int{}
		for i, col := range rec.Columns() {
			colname := rec.ColumnName(i)
			if colname != "SNP" {
				continue
			}
			d := array.NewStringData(col.Data())
			for i := 0; i < d.Len(); i++ {
				if SNPS[d.Value(i)] {
					dropped["SNP"]++
					drop_idxs = append(drop_idxs, i)
				} else {
					SNPS[d.Value(i)] = true
				}
			}
		}
		if len(drop_idxs) > 0 {
			slice_idxs := utils.Slices(int(rec.NumRows()), drop_idxs)
			drop_idxs = []int{}
			new_cols := make([]array.Interface, 0)
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			for _, col := range rec.Columns() {
				new_data := make([]array.Interface, 0)
				for _, idx := range slice_idxs {
					ns := array.NewSliceData(col.Data(), int64(idx[0]), int64(idx[1]))
					ns.Retain()
					if col.DataType().ID() == arrow.FLOAT64 {
						//log.Println(rec.ColumnName(i), " : ", col.DataType().ID().String())
						new_data = append(new_data, array.NewFloat64Data(ns))
					} else {
						//log.Println(rec.ColumnName(i), " : ", col.DataType().ID().String())
						new_data = append(new_data, array.NewStringData(ns))
					}
					ns.Release()
				}
				new_col, err := array.Concatenate(new_data, mem)
				if err != nil {
					log.Println(new_data)
					log.Println("Error:", err)
					return
				}
				new_col.Retain()
				new_cols = append(new_cols, new_col)
				new_col.Release()
			}
			new_rec := array.NewRecord(schema, new_cols, int64(new_cols[0].Len()))
			new_rec.Retain()
			records = append(records, new_rec)
			new_rec.Release()
		} else {
			records = append(records, rec)
		}
	}
	log.Println("Dropped", dropped["SNP"], "duplicate SNPS.")
	new_table = array.NewTableFromRecords(schema, records)
	return
}

func ProcessN(df array.Table, schema *arrow.Schema, narg []string) (new_table array.Table) {

	colnames := map[string]int{}
	var n float64
	var nmin float64

	for i := 0; i < int(df.NumCols()); i++ {
		colnames[df.Column(i).Name()] = i
	}

	if utils.InList("N", utils.GetKeys(colnames)) {
		log.Println("N column found.")
		tr := array.NewTableReader(df, 200)
		for tr.Next() {
		}
		//nmin = stat.Quantile(0.9, stat.Empirical, ncol_data, nil) / 1.5
		//log.Println("Nmin:", nmin)
	}

	if utils.InList("N_CON", utils.GetKeys(colnames)) {
		log.Println("N_CON column found.")
		ncon := df.Column(colnames["N_CON"]).Data()
		ncas := df.Column(colnames["N_CAS"]).Data()

		ncon_data := []float64{}
		ncas_data := []float64{}

		for _, c := range ncon.Chunks() {
			ncon_data = append(ncon_data, array.NewFloat64Data(c.Data()).Float64Values()...)
		}

		for _, c := range ncas.Chunks() {
			ncas_data = append(ncas_data, array.NewFloat64Data(c.Data()).Float64Values()...)
		}
		N := make([]float64, 0)
		floats.AddTo(N, ncon_data, ncas_data)

		P := make([]float64, len(N))
		floats.DivTo(P, ncas_data, N)

		max := make([]float64, len(N))
		maxv := floats.Max(N)
		for i := range N {
			max[i] = maxv
		}
		G := make([]float64, len(N))
		PdivMax := make([]float64, len(N))
		floats.DivTo(PdivMax, P, max)
		floats.MulTo(G, N, PdivMax)
		mean := floats.Sum(G) / float64(len(N))
		n = mean
	}

	if utils.InList("NSTUDY", utils.GetKeys(colnames)) && !utils.InList("N", utils.GetKeys(colnames)) {
		log.Println("NSTUDY column found.")
		ncol := df.Column(colnames["NSTUDY"]).Data()
		ncol_data := []float64{}
		for _, c := range ncol.Chunks() {
			ncol_data = append(ncol_data, array.NewFloat64Data(c.Data()).Float64Values()...)
		}
		nmin = floats.Max(ncol_data)
	}

	if !utils.InList("N", utils.GetKeys(colnames)) {
		var err error
		if narg[0] != "" {
			n, err = strconv.ParseFloat(narg[0], 64)
			if err != nil {
				log.Println("Error:", err)
				return
			}
			log.Println("N =", n)
		} else if narg[1] != "" {
			ncon, err := strconv.ParseFloat(narg[1], 64)
			if err != nil {
				log.Println("Error:", err)
				return
			}
			ncas, err := strconv.ParseFloat(narg[2], 64)
			if err != nil {
				log.Println("Error:", err)
				return
			}
			n = ncon + ncas
			log.Println("N =", n)
		}
	}

	records := make([]array.Record, 0)
	tr := array.NewTableReader(df, 200)

	log.Print("Processing...")
	for tr.Next() {
		rec := tr.Record()
		log.Println("Processing record...")
		rec.Retain()
		drop_idxs := []int{}
		for i, col := range rec.Columns() {
			colname := rec.ColumnName(i)
			if colname == "N" || colname == "NSTUDY" && nmin > 0 {
				log.Println("Filtering column", colname, "by N >=", nmin)
				d := array.NewFloat64Data(col.Data())
				for i, v := range d.Float64Values() {
					if v < nmin {
						drop_idxs = append(drop_idxs, i)
					}
				}
			}
		}
		if len(drop_idxs) > 0 {
			slice_idxs := utils.Slices(int(rec.NumRows()), drop_idxs)
			drop_idxs = []int{}
			new_cols := make([]array.Interface, 0)
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			for _, col := range rec.Columns() {
				new_data := make([]array.Interface, 0)
				for _, idx := range slice_idxs {
					ns := array.NewSliceData(col.Data(), int64(idx[0]), int64(idx[1]))
					ns.Retain()
					new_data = append(new_data, array.NewFloat64Data(ns))
					ns.Release()
				}
				new_col, err := array.Concatenate(new_data, mem)
				if err != nil {
					log.Println(new_data)
					log.Println("Error:", err)
					return
				}
				new_col.Retain()
				new_cols = append(new_cols, new_col)
				new_col.Release()
			}
			new_rec := array.NewRecord(schema, new_cols, int64(new_cols[0].Len()))
			new_rec.Retain()
			records = append(records, new_rec)
			new_rec.Release()
		} else {
			data := make([]float64, rec.NumRows())
			for i := range data {
				data[i] = n
			}
			fields := make([]arrow.Field, 0)
			fixed := false
			for _, c := range schema.Fields() {
				if utils.InList(c.Name, []string{"N", "NSTUDY", "N_CON", "N_CAS"}) && !fixed {
					c = arrow.Field{Name: "N", Type: arrow.PrimitiveTypes.Float64, Nullable: true, Metadata: arrow.Metadata{}}
					fixed = true
				}
				fields = append(fields, c)
			}
			nschema := arrow.NewSchema(fields, nil)
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			nrbld := array.NewRecordBuilder(mem, nschema)
			defer nrbld.Release()
			for i := range nrbld.Fields() {
				if nschema.Field(i).Type.Name() == "FLOAT64" {
					if nschema.Field(i).Name == "N" {
						nrbld.Field(i).(*array.Float64Builder).AppendValues(data, nil)
					} else {
						nrbld.Field(i).(*array.Float64Builder).AppendValues(array.NewFloat64Data(rec.Column(i).Data()).Float64Values(), nil)
					}
				} else {
					strs := make([]string, rec.NumRows())
					for i := 0; int64(i) < rec.NumRows(); i++ {
						strs = append(strs, array.NewStringData(rec.Column(i).Data()).Value(i))
					}
					nrbld.Field(i).(*array.StringBuilder).AppendValues(strs, nil)
				}
			}
			rec := nrbld.NewRecord()
			records = append(records, rec)
		}
	}
	new_table = array.NewTableFromRecords(schema, records)
	new_table.Retain()
	return
}
