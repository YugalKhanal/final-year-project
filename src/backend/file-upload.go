package main

import (
	// "encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func RouteHandler() {
	http.HandleFunc("/upload", uploadHandler)
	http.Handle("/", http.FileServer(http.Dir("../front-end/"))) // Serve HTML front-end
	fmt.Println("Server running at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// func uploadHandler(w http.ResponseWriter, r *http.Request) {
// 	// Ensure the "uploads" directory exists
// 	uploadsDir := "uploads"
// 	if _, err := os.Stat(uploadsDir); os.IsNotExist(err) {
// 		err := os.Mkdir(uploadsDir, os.ModePerm)
// 		if err != nil {
// 			http.Error(w, "Unable to create uploads directory", http.StatusInternalServerError)
// 			return
// 		}
// 	}
//
// 	file, header, err := r.FormFile("file") // Make sure this matches your HTML form field name
// 	if err != nil {
//     // w.Write(nil)
// 		http.Error(w, "Unable to read file", http.StatusBadRequest)
// 		fmt.Println(err)
// 		return
// 	}
// 	defer file.Close()
//
// 	// Define the destination path
// 	dst := filepath.Join(uploadsDir, header.Filename)
// 	out, err := os.Create(dst)
// 	if err != nil {
// 		http.Error(w, "Unable to save file", http.StatusInternalServerError)
// 		return
// 	}
// 	defer out.Close()
//
// 	// Write the file to the destination path
//   // (out, file) -> (destination(path), source)
// 	_, err = io.Copy(out, file)
// 	if err != nil {
// 		http.Error(w, "Error saving file", http.StatusInternalServerError)
// 		return
// 	}
//
// 	w.WriteHeader(http.StatusOK)
// 	fmt.Fprintf(w, "File uploaded successfully: %s\n", header.Filename)
// }


func uploadHandler(w http.ResponseWriter, r *http.Request){
	// Ensure the "uploads" directory exists
	uploadsDir := "uploads"
	if _, err := os.Stat(uploadsDir); os.IsNotExist(err) {
		err := os.Mkdir(uploadsDir, os.ModePerm)
		if err != nil {
			http.Error(w, "Unable to create uploads directory", http.StatusInternalServerError)
			return
		}
	}

	file, header, err := r.FormFile("file") // Make sure this matches your HTML form field name
	if err != nil {
    // w.Write(nil)
		http.Error(w, "Unable to read file", http.StatusBadRequest)
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Define the destination path
	dst := filepath.Join(uploadsDir, header.Filename)
	out, err := os.Create(dst)
	if err != nil {
		http.Error(w, "Unable to save file", http.StatusInternalServerError)
		return
	}
	defer out.Close()

	// Write the file to the destination path
  // (out, file) -> (destination(path), source)
	_, err = io.Copy(out, file)
	if err != nil {
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "File uploaded successfully: %s\n", header.Filename)
}
