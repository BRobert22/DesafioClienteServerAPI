package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Response struct {
	USDBRL struct {
		Code       string `json:"code"`
		Codein     string `json:"codein"`
		Name       string `json:"name"`
		High       string `json:"high"`
		Low        string `json:"low"`
		VarBid     string `json:"varBid"`
		PctChange  string `json:"pctChange"`
		Bid        string `json:"bid"`
		Ask        string `json:"ask"`
		Timestamp  string `json:"timestamp"`
		CreateDate string `json:"create_date"`
	} `json:"USDBRL"`
}

type ResponseEndpoint struct {
	Bid string `json:"bid"`
}

func main() {
	connectionString := "root:root@tcp(localhost:3306)/goexpert?charset=utf8mb4&parseTime=True&loc=Local"
	context, err := sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer context.Close()

	err = context.Ping()
	if err != nil {
		log.Fatalf("Erro ao conectar ao banco de dados: %v", err)
	}

	criarTabela(context)

	http.HandleFunc("/cotacao", func(w http.ResponseWriter, r *http.Request) {
		obterCotacao(w, context)
	})

	fmt.Println("Servidor rodando na porta 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func criarTabela(context *sql.DB) {
	comandoSQL := `
	CREATE TABLE IF NOT EXISTS cotacoes (
		id INT AUTO_INCREMENT PRIMARY KEY,
		bid VARCHAR(50) NOT NULL,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := context.Exec(comandoSQL)
	if err != nil {
		log.Printf("Erro ao criar tabela: %v", err)
	}
}

func obterCotacao(w http.ResponseWriter, bancoDados *sql.DB) {
	ctxAPI, cancelarRequisicao := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelarRequisicao()

	cotacao, err := buscarCotacao(ctxAPI)
	if err != nil {
		log.Printf("Erro ao buscar cotação: %v", err)
		http.Error(w, "Erro ao buscar cotação", http.StatusInternalServerError)
		return
	}

	ctxBD, cancelar := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelar()

	err = salvarCotacao(ctxBD, bancoDados, cotacao.USDBRL.Bid)
	if err != nil {
		log.Printf("Erro ao salvar no banco de dados: %v", err)
	}

	resposta := ResponseEndpoint{Bid: cotacao.USDBRL.Bid}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resposta)
}

func buscarCotacao(ctx context.Context) (*Response, error) {
	requisicao, err := http.NewRequestWithContext(ctx, "GET", "https://economia.awesomeapi.com.br/json/last/USD-BRL", nil)
	if err != nil {
		return nil, err
	}

	resposta, err := http.DefaultClient.Do(requisicao)
	if err != nil {
		return nil, err
	}
	defer resposta.Body.Close()

	corpo, err := io.ReadAll(resposta.Body)
	if err != nil {
		return nil, err
	}

	var cotacao Response
	err = json.Unmarshal(corpo, &cotacao)
	if err != nil {
		return nil, err
	}

	return &cotacao, nil
}

func salvarCotacao(ctx context.Context, bancoDados *sql.DB, bid string) error {
	stmt, err := bancoDados.PrepareContext(ctx, "INSERT INTO cotacoes(bid) VALUES(?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, bid)
	if err != nil {
		return err
	}

	return nil
}
