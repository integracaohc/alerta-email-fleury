package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net/smtp"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/godror/godror"
	"github.com/joho/godotenv"
)

var (
	smtpHost = "smtp.office365.com"
	smtpPort = "587"
	smtpUser = os.Getenv("loginEmail")
	smtpPass = os.Getenv("passwordEmail")

	emailFrom = os.Getenv("loginEmail")
	emailTo   = os.Getenv("emails")
)

type QueryConfig struct {
	Name  string
	Query string
}

func main() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Println("Adicionar o arquivo de configuração .env \n", err.Error())
		panic(err)
	}
	dbuser := os.Getenv("dbuser")
	dbpassword := os.Getenv("dbpassword")
	dbname := os.Getenv("dbname")
	dbhost := os.Getenv("dbhost")
	dbport := os.Getenv("dbport")

	dbURI := fmt.Sprintf("%s/%s@%s:%s/%s", dbuser, dbpassword, dbhost, dbport, dbname)
	fmt.Println(dbURI)
	db, err := sql.Open("godror", dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	queries := []QueryConfig{
		{
			Name: "RESULTADO_EXAME_CULTURA",
			Query: `
				SELECT
					(select cd_pedido from mvintegra.imv_resultado_pssd where cd_imv_log_requisicao = t.cd_imv_log_requisicao and ROWNUM = 1) as cd_pedido,
					t.cd_imv_log_requisicao, t.dh_criacao_log, t.nm_servico, t.sn_sucesso, t.cd_sistema_origem, t.cd_empresa_destino,
					t.DS_ERRO,
					CASE
						WHEN INSTR(LOWER(t.DS_ERRO), 'cadastrado') > 0 THEN 'Time Cadastro'
						WHEN INSTR(LOWER(t.DS_ERRO), 'entrar em contato a mv-sistemas') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'violada - chave mae nao localizada') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'erro durante a execucao do gatilho') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor nulo') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor muito grande') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'nao possuem resultado') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'localizado no mv2000i') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'a imagem') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'inserir null') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'extrair os parametros') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'resposta do webservice') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'Ocorreu um erro ao tentar conex') > 0 THEN 'FLEURY'
						ELSE ''
					END AS TIME_RESPONSAVEL
				FROM MVINTEGRA.IMV_LOG_REQUISICAO t
				WHERE t.nm_servico = 'RESULTADO_EXAME_CULTURA'
				AND t.sn_sucesso = 'N'
				--AND t.dh_criacao_log >= TRUNC(SYSDATE - 1)
                AND (
                    /* Segunda-feira → pega sexta, sábado e domingo */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') = '2'
                     AND t.dh_criacao_log >= TRUNC(SYSDATE - 3))
                
                    OR
                
                    /* Terça a sexta → pega somente ontem */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') BETWEEN '3' AND '6'
                     AND t.dh_criacao_log >= TRUNC(SYSDATE - 1))
                    )
				AND t.DS_ERRO NOT LIKE '%pois ja existe uma amostra com o mesmo codigo%'
			`,
		},
		{
			Name: "RESULTADO_EXAME",
			Query: `
				SELECT
					(select cd_pedido from mvintegra.imv_resultado_pssd where cd_imv_log_requisicao = t.cd_imv_log_requisicao and ROWNUM = 1) as cd_pedido,
					t.cd_imv_log_requisicao, t.dh_criacao_log, t.nm_servico, t.sn_sucesso, t.cd_sistema_origem, t.cd_empresa_destino,
					t.DS_ERRO,
					CASE
						WHEN INSTR(LOWER(t.DS_ERRO), 'cadastrado') > 0 THEN 'Time Cadastro'
						WHEN INSTR(LOWER(t.DS_ERRO), 'entrar em contato a mv-sistemas') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'violada - chave mae nao localizada') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'erro durante a execucao do gatilho') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor nulo') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor muito grande') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'nao possuem resultado') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'localizado no mv2000i') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'a imagem') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'inserir null') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'extrair os parametros') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'resposta do webservice') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'Ocorreu um erro ao tentar conex') > 0 THEN 'FLEURY'
						ELSE ''
					END AS TIME_RESPONSAVEL
				FROM MVINTEGRA.IMV_LOG_REQUISICAO t
				WHERE t.nm_servico = 'RESULTADO_EXAME'
				AND t.sn_sucesso = 'N'
				--AND t.dh_criacao_log >= TRUNC(SYSDATE - 1)
                AND (
                    /* Segunda-feira → pega sexta, sábado e domingo */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') = '2'
                     AND t.dh_criacao_log >= TRUNC(SYSDATE - 3))
                
                    OR
                
                    /* Terça a sexta → pega somente ontem */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') BETWEEN '3' AND '6'
                     AND t.dh_criacao_log >= TRUNC(SYSDATE - 1))
                    )
				AND t.DS_ERRO NOT LIKE '%pois ja existe uma amostra com o mesmo codigo%'
			`,
		},
		{
			Name: "AMOSTRA_EXAME",
			Query: `
				SELECT
					(select cd_pedido from mvintegra.imv_resultado_pssd where cd_imv_log_requisicao = t.cd_imv_log_requisicao and ROWNUM = 1) as cd_pedido,
					t.cd_imv_log_requisicao, t.dh_criacao_log, t.nm_servico, t.sn_sucesso, t.cd_sistema_origem, t.cd_empresa_destino,
					t.DS_ERRO,
					CASE
						WHEN INSTR(LOWER(t.DS_ERRO), 'cadastrado') > 0 THEN 'Time Cadastro'
						WHEN INSTR(LOWER(t.DS_ERRO), 'entrar em contato a mv-sistemas') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'violada - chave mae nao localizada') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'erro durante a execucao do gatilho') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor nulo') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor muito grande') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'nao possuem resultado') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'localizado no mv2000i') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'a imagem') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'inserir null') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'extrair os parametros') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'resposta do webservice') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'Ocorreu um erro ao tentar conex') > 0 THEN 'FLEURY'
						ELSE ''
					END AS TIME_RESPONSAVEL
				FROM MVINTEGRA.IMV_LOG_REQUISICAO t
				WHERE t.nm_servico = 'AMOSTRA_EXAME'
				AND t.sn_sucesso = 'N'
				--AND t.dh_criacao_log >= TRUNC(SYSDATE - 1)
                AND (
                    /* Segunda-feira → pega sexta, sábado e domingo */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') = '2'
                     AND t.dh_criacao_log >= TRUNC(SYSDATE - 3))
                
                    OR
                
                    /* Terça a sexta → pega somente ontem */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') BETWEEN '3' AND '6'
                     AND t.dh_criacao_log >= TRUNC(SYSDATE - 1))
                    )	
				AND t.DS_ERRO NOT LIKE '%pois ja existe uma amostra com o mesmo codigo%'
			`,
		},
		{
			Name: "PEDIDO_EXAME_SADT",
			Query: `
				SELECT
                    sp.CD_IDENTIFICADOR as cd_pedido,
					t.cd_imv_mensagem_saida_formtd, t.tp_status, t.cd_sistema_destino, t.tp_documento, t.dh_criacao_msg,
					t.DS_ERRO,
					CASE
						WHEN INSTR(LOWER(t.DS_ERRO), 'cadastrado') > 0 THEN 'Time Cadastro'
						WHEN INSTR(LOWER(t.DS_ERRO), 'entrar em contato a mv-sistemas') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'violada - chave mae nao localizada') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'erro durante a execucao do gatilho') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor nulo') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'valor muito grande') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'nao possuem resultado') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'localizado no mv2000i') > 0 THEN 'Time MV'
						WHEN INSTR(LOWER(t.DS_ERRO), 'a imagem') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'inserir null') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'extrair os parametros') > 0 THEN 'FLEURY'
						WHEN INSTR(LOWER(t.DS_ERRO), 'resposta do webservice') > 0 THEN 'FLEURY'
                        WHEN INSTR(LOWER(t.DS_ERRO), 'Ocorreu um erro ao tentar conex') > 0 THEN 'FLEURY'
						ELSE ''
					END AS TIME_RESPONSAVEL
				FROM MVINTEGRA.IMV_MENSAGEM_SAIDA_FORMATADA t, MVINTEGRA.IMV_MENSAGEM_SAIDA_PADRAO SP
				WHERE t.tp_documento = 'PEDIDO_EXAME_SADT'
				AND t.tp_status = 'E'
				--AND t.dh_criacao_msg >= TRUNC(SYSDATE - 1)
                AND (
                    /* Segunda-feira → pega sexta, sábado e domingo */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') = '2'
                     AND t.dh_criacao_msg >= TRUNC(SYSDATE - 3))
                
                    OR
                
                    /* Terça a sexta → pega somente ontem */
                    (TO_CHAR(TRUNC(SYSDATE), 'D', 'NLS_DATE_LANGUAGE=AMERICAN') BETWEEN '3' AND '6'
                     AND t.dh_criacao_msg >= TRUNC(SYSDATE - 1))
                    )
				AND t.DS_ERRO NOT LIKE '%pois ja existe uma amostra com o mesmo codigo%'
                and t.CD_IMV_MENSAGEM_SAIDA_PADRAO = SP.CD_IMV_MENSAGEM_SAIDA_PADRAO
			`,
		},
	}

	var arquivosFleury []string
	var arquivosOutros []string

	for _, q := range queries {
		file, err := executarQueryCSV(ctx, db, q)
		if err != nil {
			log.Printf("Erro na query %s: %v", q.Name, err)
			continue
		}

		fleury, outros, err := divideArquivos(file)
		if err != nil {
			log.Printf("Erro ao dividir arquivo %s: %v", file, err)
			continue
		}

		if fleury != "" {
			arquivosFleury = append(arquivosFleury, fleury)
		}
		if outros != "" {
			arquivosOutros = append(arquivosOutros, outros)
		}
	}

	toFleury := strings.Split(os.Getenv("emails_fleury"), ";")
	//[]string{"emailreport@hospitalcare.com.br"}
	toOutros := strings.Split(os.Getenv("emails_outros"), ";")

	if len(arquivosFleury) > 0 {
		err = enviarEmail(arquivosFleury, toFleury, "Núcleo Técnico")
		if err != nil {
			log.Fatal("Erro ao enviar email FLEURY:", err)
		}
	}

	if len(arquivosOutros) > 0 {
		err = enviarEmail(arquivosOutros, toOutros, "Suporte")
		if err != nil {
			log.Fatal("Erro ao enviar email OUTROS:", err)
		}
	}

	for _, f := range append(arquivosFleury, arquivosOutros...) {
		_ = os.Remove(f)
	}
}

func divideArquivos(filePath string) (string, string, error) {

	// Abre o CSV de entrada
	inputFile, err := os.Open(filePath)
	if err != nil {
		return "", "", err
	}
	defer inputFile.Close()

	reader := csv.NewReader(inputFile)
	reader.Comma = ',' // ajuste se necessário

	// Lê o cabeçalho
	header, err := reader.Read()
	if err != nil {
		return "", "", err
	}

	// Descobre o índice da coluna TIME_RESPONSAVEL
	colIndex := -1
	for i, col := range header {
		if strings.EqualFold(col, "TIME_RESPONSAVEL") {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return "", "", fmt.Errorf("coluna TIME_RESPONSAVEL não encontrada")
	}

	// Monta nomes dos arquivos
	nomeBase := strings.TrimSuffix(filePath, filepath.Ext(filePath))
	ext := filepath.Ext(filePath)

	arquivoFleury := fmt.Sprintf("%s_erros_fleury%s", nomeBase, ext)
	arquivoOutros := fmt.Sprintf("%s_erros_outros%s", nomeBase, ext)

	// Cria arquivos de saída
	fileFleury, err := os.Create(arquivoFleury)
	if err != nil {
		return "", "", err
	}
	defer fileFleury.Close()

	fileOutros, err := os.Create(arquivoOutros)
	if err != nil {
		return "", "", err
	}
	defer fileOutros.Close()

	writerFleury := csv.NewWriter(fileFleury)
	writerOutros := csv.NewWriter(fileOutros)

	// Escreve cabeçalho
	_ = writerFleury.Write(header)
	_ = writerOutros.Write(header)

	// Contadores
	countFleury := 0
	countOutros := 0

	// Processa linhas
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return "", "", err
		}

		valor := strings.TrimSpace(strings.ToUpper(record[colIndex]))

		if valor == "FLEURY" {
			_ = writerFleury.Write(record)
			countFleury++
		} else {
			_ = writerOutros.Write(record)
			countOutros++
		}
	}

	writerFleury.Flush()
	writerOutros.Flush()

	var fleuryFile, outrosFile string

	// Remove arquivo se estiver vazio
	if countFleury == 0 {
		_ = os.Remove(arquivoFleury)
	} else {
		fleuryFile = arquivoFleury
	}

	if countOutros == 0 {
		_ = os.Remove(arquivoOutros)
	} else {
		outrosFile = arquivoOutros
	}

	return fleuryFile, outrosFile, nil
}

func executarQueryCSV(ctx context.Context, db *sql.DB, cfg QueryConfig) (string, error) {
	rows, err := db.QueryContext(ctx, cfg.Query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	fileName := fmt.Sprintf(
		"%s_%s.csv",
		cfg.Name,
		time.Now().Format("20060102_150405"),
	)

	filePath := filepath.Join(os.TempDir(), fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	_ = writer.Write(cols)

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", err
		}

		record := make([]string, len(cols))
		for i, v := range values {
			if v != nil {
				record[i] = fmt.Sprint(v)
			}
		}
		_ = writer.Write(record)
	}

	return filePath, nil
}

type loginAuth struct {
	username, password string
}

// LoginAuth funcao para autenticacao no office365
func LoginAuth(username, password string) smtp.Auth {
	return &loginAuth{username, password}
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if more {
		switch string(fromServer) {
		case "Username:":
			return []byte(a.username), nil
		case "Password:":
			return []byte(a.password), nil
		default:
			return nil, errors.New("Unknown from server")
		}
	}
	return nil, nil
}

type smtpServer struct {
	host string
	port string
}

// serverName URI to smtp server
func (s *smtpServer) serverName() string {
	return s.host + ":" + s.port
}

func enviarEmail(arquivos []string, emailsTo []string, time string) error {
	boundary := "BOUNDARY_FLEURY_123456"

	subject := "Relatório Erros Fleury - Registros com erro (D-1)"

	header := ""
	header += fmt.Sprintf("From: %s\r\n", emailFrom)
	header += fmt.Sprintf("To: %s\r\n", emailTo)
	header += fmt.Sprintf("Subject: %s\r\n", subject)
	header += "MIME-Version: 1.0\r\n"
	header += fmt.Sprintf("Content-Type: multipart/mixed; boundary=%s\r\n", boundary)
	header += "\r\n"

	body := ""
	body += fmt.Sprintf("--%s\r\n", boundary)
	body += "Content-Type: text/plain; charset=utf-8\r\n"
	body += "Content-Transfer-Encoding: 7bit\r\n\r\n"
	body += "Segue em anexo os arquivos CSV gerados automaticamente do dia anterior.\r\n\r\n"

	//fmt.Println("Enviando email com arquivos:", arquivos)
	// Anexos
	for _, filePath := range arquivos {
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		fileName := filepath.Base(filePath)
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(fileBytes)))
		base64.StdEncoding.Encode(encoded, fileBytes)

		body += fmt.Sprintf("--%s\r\n", boundary)
		body += "Content-Type: text/csv\r\n"
		body += "Content-Transfer-Encoding: base64\r\n"
		body += fmt.Sprintf("Content-Disposition: attachment; filename=\"%s\"\r\n\r\n", fileName)

		// quebra em linhas de 76 chars (padrão MIME)
		for i := 0; i < len(encoded); i += 76 {
			end := i + 76
			if end > len(encoded) {
				end = len(encoded)
			}
			body += string(encoded[i:end]) + "\r\n"
		}
	}

	body += fmt.Sprintf("--%s--", boundary)
	//fmt.Println("Email montado:", header+body)

	msg := []byte(header + body)

	smtpSrv := smtpServer{host: smtpHost, port: smtpPort}

	emailFrom = os.Getenv("loginEmail")
	smtpPass = os.Getenv("passwordEmail")

	auth := LoginAuth(emailFrom, smtpPass)

	// to := []string{ //[]string{
	// 	"emailreport@hospitalcare.com.br",
	// }
	to := emailsTo

	err := smtp.SendMail(
		smtpSrv.serverName(),
		auth,
		emailFrom,
		to,
		msg,
	)
	if err != nil {
		fmt.Println("Erro ao enviar email:", err)
		return err
	}

	fmt.Println("Email enviado com sucesso para", time)
	return nil
}
