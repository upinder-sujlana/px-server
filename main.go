package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-playground/validator/v10"
	_ "github.com/go-sql-driver/mysql"
)

type Node struct {
	NodeID        string `json:"nodeID" validate:"required"`
	NodeIP        string `json:"nodeIP" validate:"required"`
	NodeKernel    string `json:"nodeKernel" validate:"required"`
	NodeOS        string `json:"nodeOS" validate:"required"`
	NodePxVersion string `json:"nodePxVersion" validate:"required"`
}

var (
	db       *sql.DB
	validate *validator.Validate
)

func main() {
	// Read env vars
	user := os.Getenv("MYSQL_USER")
	pass := os.Getenv("MYSQL_PASSWORD")
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	dbName := os.Getenv("MYSQL_DB")
	if user == "" || pass == "" || host == "" || port == "" || dbName == "" {
		log.Fatal("Missing required MySQL environment variables")
	}

	// Connect without DB to create DB if needed
	dsnNoDB := fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, pass, host, port)
	tmpDB, err := sql.Open("mysql", dsnNoDB)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL server: %v", err)
	}
	defer tmpDB.Close()

	_, err = tmpDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	log.Printf("Database %s ensured.", dbName)

	// Now connect to the DB
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, pass, host, port, dbName)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL DB: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping MySQL DB: %v", err)
	}
	log.Println("Connected to MySQL DB!")

	// Create table if not exists
	createTable := `
	CREATE TABLE IF NOT EXISTS nodes (
		nodeID VARCHAR(64) PRIMARY KEY,
		nodeIP VARCHAR(64),
		nodeKernel VARCHAR(128),
		nodeOS VARCHAR(128),
		nodePxVersion VARCHAR(64)
	)`
	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	validate = validator.New()

	http.HandleFunc("/node", nodeHandler)
	http.HandleFunc("/nodes", nodesHandler)
	log.Println("Server running at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func nodeHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		var n Node
		if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if err := validate.Struct(n); err != nil {
			http.Error(w, "Missing or invalid fields: "+err.Error(), http.StatusBadRequest)
			return
		}
		_, err := db.Exec(`INSERT INTO nodes (nodeID, nodeIP, nodeKernel, nodeOS, nodePxVersion)
			VALUES (?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE nodeIP=VALUES(nodeIP), nodeKernel=VALUES(nodeKernel), nodeOS=VALUES(nodeOS), nodePxVersion=VALUES(nodePxVersion)`,
			n.NodeID, n.NodeIP, n.NodeKernel, n.NodeOS, n.NodePxVersion)
		if err != nil {
			http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintln(w, "Node saved")
	case "GET":
		nodeID := r.URL.Query().Get("id")
		if nodeID == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}
		var n Node
		err := db.QueryRow(`SELECT nodeID, nodeIP, nodeKernel, nodeOS, nodePxVersion FROM nodes WHERE nodeID = ?`, nodeID).
			Scan(&n.NodeID, &n.NodeIP, &n.NodeKernel, &n.NodeOS, &n.NodePxVersion)
		if err == sql.ErrNoRows {
			http.Error(w, "Node not found", http.StatusNotFound)
			return
		} else if err != nil {
			http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(n)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func nodesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rows, err := db.Query(`SELECT nodeID, nodeIP, nodeKernel, nodeOS, nodePxVersion FROM nodes`)
	if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.NodeID, &n.NodeIP, &n.NodeKernel, &n.NodeOS, &n.NodePxVersion); err != nil {
			http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		nodes = append(nodes, n)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}
