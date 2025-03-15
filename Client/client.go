package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type CotacaoResponse struct {
	Bid string `json:"bid"`
}

func main() {
	ctx, cancelar := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelar()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost:8080/cotacao", nil)
	if err != nil {
		log.Fatalf("Erro ao criar requisição: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Erro ao fazer requisição: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Erro ao ler resposta: %v", err)
	}

	var cotacao CotacaoResponse
	err = json.Unmarshal(body, &cotacao)
	if err != nil {
		log.Fatalf("Erro ao fazer parse do JSON: %v", err)
	}

	err = salvaNoArquivo(cotacao.Bid)
	if err != nil {
		log.Fatalf("Erro ao salvar arquivo: %v", err)
	}

	fmt.Printf("Cotação do dólar: %s\n", cotacao.Bid)
	fmt.Println("Cotação salva com sucesso no arquivo 'cotacao.txt'")
}

func salvaNoArquivo(bid string) error {
	file, err := os.Create("cotacao.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("Dólar: %s", bid))
	if err != nil {
		return err
	}

	return nil
}
