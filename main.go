package main

import (
	"encoding/csv"
	"fmt"
	"github.com/openact/kit/cache"
	"github.com/openact/kit/sys"
	"github.com/openact/tblAnalyzer/conf"
	"github.com/schollz/progressbar/v3"
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
	fmt.Printf("Processing task: %s\n", task.Name)

	// Collect data
	idxCount := make(map[string]int)
	paths := make([]string, 0)
	tblInfos := make([]*conf.TblInfo, 0, 1000)
	idxSet := make(map[string]struct{})
	tblMapIdx := make(map[string]map[string]bool)

	// First collect all paths to set up progress bar
	for _, dir := range task.Dirs {
		dirFilePaths := sys.GetFilePaths(dir, task.IfRecursive)
		paths = append(paths, dirFilePaths...)
	}

	// Create progress bar for file processing
	bar := progressbar.NewOptions(len(paths),
		progressbar.OptionSetDescription("Processing files"),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetWidth(50),
	)

	// Process each file
	for _, filePath := range paths {
		// only *.fac, *.txt, *.csv files
		if !strings.HasSuffix(filePath, ".fac") && !strings.HasSuffix(filePath, ".txt") && !strings.HasSuffix(filePath, ".csv") {
			log.Printf("Skipping file with unsupported extension: %s (supported: .fac, .txt, .csv)", filePath)
			bar.Add(1)
			continue
		}

		tbl := cache.ParseGenericTable(filePath)

		fileSize, err := sys.GetFileSize(tbl.FilePath)
		timeStamp := sys.GetTimeStamp(tbl.FilePath)
		if err != nil {
			return fmt.Errorf("failed to get file size: %v", err)
		}
		tblInfos = append(tblInfos, &conf.TblInfo{
			sys.GetFileName(tbl.FilePath),
			tbl.FilePath,
			fileSize,
			timeStamp,
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

		bar.Add(1)
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

	fmt.Println("\nWriting table_info.csv...")
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

	// Create progress bar for table info
	bar = progressbar.NewOptions(len(tblInfos),
		progressbar.OptionSetDescription("Writing Summary "),
		progressbar.OptionSetWidth(50),
		progressbar.OptionShowCount(),
	)

	// Write header
	header := []string{"Table Path", "Modified at", "Table Size (in M)", "Table Name", "Indexes", "ColKeys"}
	if err := writer.Write(header); err != nil {
		csvFile.Close()
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Write data rows
	for _, tblInfo := range tblInfos {
		row := []string{tblInfo.TblPath, tblInfo.TimeStamp, fmt.Sprintf("%.2f", float64(tblInfo.TblSize)/1024/1024), tblInfo.TblName}
		row = append(row, strings.Join(tblInfo.IndexNames, ";"))
		row = append(row, strings.Join(tblInfo.ColKeys, ";"))
		if err := writer.Write(row); err != nil {
			csvFile.Close()
			return fmt.Errorf("failed to write row: %v", err)
		}
		bar.Add(1)
	}
	writer.Flush()
	csvFile.Close()

	fmt.Println("\nWriting table_index_analysis.csv...")
	// Write CSV - Pivot
	csvPath = filepath.Join(taskDir, "table_index_analysis.csv")
	csvFile, err = os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	writer = csv.NewWriter(csvFile)

	// Create progress bar for pivot table
	bar = progressbar.NewOptions(len(tblInfos),
		progressbar.OptionSetDescription("Writing pivot   "),
		progressbar.OptionSetWidth(50),
		progressbar.OptionShowCount(),
	)

	// Write header
	header = append([]string{"Table Path"}, indexes...)
	if err := writer.Write(header); err != nil {
		csvFile.Close()
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Write data rows
	for _, info := range tblInfos {

		row := make([]string, 0, len(indexes)+1)
		row = append(row, info.TblPath)
		for _, idx := range indexes {
			if tblMapIdx[info.TblPath][idx] {
				row = append(row, "1")
			} else {
				row = append(row, "0")
			}
		}
		if err := writer.Write(row); err != nil {
			csvFile.Close()
			return fmt.Errorf("failed to write row: %v", err)
		}
		bar.Add(1)
	}

	writer.Flush()
	csvFile.Close()

	log.Printf("Found %d unique indexes across all tables", len(indexes))
	return nil
}
