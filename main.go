package main

import (
	"encoding/csv"
	"fmt"
	"github.com/openact/kit/cache"
	"github.com/openact/kit/sys"
	"github.com/openact/tblAnalyzer/conf"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	sys.SetupLog()
	cfg, err := conf.LoadConfig("inputs/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	for _, task := range cfg.Tasks {
		log.Printf("Running Task: %v", task)
		if err := processTask(task, cfg.OutputDir); err != nil {
			log.Fatalf("Task failed: %v", err)
		}
	}
}

func processTask(task conf.Task, outputDir string) error {
	// Collect data
	idxCount := make(map[string]int)
	paths := make([]string, 0)
	tblInfos := make([]*conf.TblInfo, 0, 1000)
	idxSet := make(map[string]struct{})
	tblMapIdx := make(map[string]map[string]bool)

	//iterate dirs
	for _, dir := range task.Dirs {
		dirFilePaths := sys.GetFilePaths(dir, task.IfRecursive)
		paths = append(paths, dirFilePaths...)
		for _, filePath := range dirFilePaths {
			// only *.fac, *.txt, *.csv files
			if !strings.HasSuffix(filePath, ".fac") && !strings.HasSuffix(filePath, ".txt") && !strings.HasSuffix(filePath, ".csv") {
				log.Printf("Skipping file with unsupported extension: %s (supported: .fac, .txt, .csv)", filePath)
				continue
			}

			tbl := cache.ParseGenericTable(filePath)
			//

			fileSize, err := sys.GetFileSize(tbl.FilePath)
			if err != nil {
				return fmt.Errorf("failed to get file size: %v", err)
			}
			tblInfos = append(tblInfos, &conf.TblInfo{
				sys.GetFileName(tbl.FilePath),
				tbl.FilePath,
				fileSize,
				tbl.Indexes,
				tbl.ColKeys,
			})

			//for pivot
			tblMapIdx[filePath] = make(map[string]bool)
			for _, idx := range tbl.Indexes {
				idxCount[idx]++
				idxSet[idx] = struct{}{}
				tblMapIdx[filePath][idx] = true
			}
		}
	}

	// Prepare data
	indexes := make([]string, 0, len(idxSet))
	for idx := range idxCount {
		indexes = append(indexes, idx)
	}
	sort.Strings(indexes)
	sort.Strings(paths)

	// Create output directory
	taskDir := filepath.Join(outputDir, task.Name)

	if err := sys.CreateDir(taskDir); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Write CSV - Table Info
	csvPath := filepath.Join(taskDir, "table_info.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	writer := csv.NewWriter(csvFile)
	//sort tblInfos by table name
	sort.Slice(tblInfos, func(i, j int) bool {
		return tblInfos[i].TblName < tblInfos[j].TblName
	})

	// Write header
	header := []string{"Table Path", "Table Size (in M)", "Table Name", "Indexes", "ColKeys"}
	if err := writer.Write(header); err != nil {
		csvFile.Close()
		return fmt.Errorf("failed to write header: %v", err)
	}
	// Write data rows
	for _, tblInfo := range tblInfos {
		row := []string{tblInfo.TblPath, fmt.Sprintf("%.2f M", float64(tblInfo.TblSize)/1024/1024), tblInfo.TblName}
		row = append(row, strings.Join(tblInfo.IndexNames, ";"))
		row = append(row, strings.Join(tblInfo.ColKeys, ";"))
		if err := writer.Write(row); err != nil {
			csvFile.Close()
			return fmt.Errorf("failed to write row: %v", err)
		}
	}
	writer.Flush()

	// Write CSV - Pivot
	csvPath = filepath.Join(taskDir, "table_index_analysis.csv")
	csvFile, err = os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	writer = csv.NewWriter(csvFile)

	// Write header
	header = append([]string{"Table Path"}, indexes...)
	if err := writer.Write(header); err != nil {
		csvFile.Close()
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Write data rows
	for _, path := range paths {
		row := make([]string, 0, len(indexes)+1)
		row = append(row, path)
		for _, idx := range indexes {
			if tblMapIdx[path][idx] {
				row = append(row, "1")
			} else {
				row = append(row, "0")
			}
		}
		if err := writer.Write(row); err != nil {
			csvFile.Close()
			return fmt.Errorf("failed to write row: %v", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		csvFile.Close()
		return fmt.Errorf("error flushing writer: %v", err)
	}

	if err := csvFile.Close(); err != nil {
		return fmt.Errorf("error closing file: %v", err)
	}

	log.Printf("Found %d unique indexes across all tables", len(indexes))
	return nil
}
