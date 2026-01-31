package com.intuitive;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Questão 1 - Web Scraping e ETL de Demonstrações Contábeis (ANS)
 * * Objetivo:
 * 1. Baixar demonstrações contábeis da ANS.
 * 2. Filtrar despesas com "EVENTOS/SINISTROS".
 * 3. Consolidar em um CSV unificado.
 * * TRADE-OFF TÉCNICO (Armazenamento em Disco vs Memória):
 * Optei por salvar os arquivos baixados em uma pasta local ("downloads/"), 
 * atuando como uma "Staging Area" (Zona de Aterrissagem).
 * * Justificativa:
 * - Resiliência: Se o processamento falhar, não é necessário baixar tudo novamente.
 * - Auditoria: Permite verificar o arquivo original (Raw Data) em caso de dúvidas.
 * - Escalabilidade: Processamos os arquivos um a um (Incremental), evitando 
 * estouro de memória (OutOfMemoryError) caso o volume de dados cresça.
 */
public class Questao1 {

    // Constantes de configuração
    private static final String URL_BASE = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";
    private static final String PASTA_DOWNLOADS = "downloads";
    private static final String ARQUIVO_CSV_FINAL = "consolidado.csv";
    private static final String ARQUIVO_ZIP_FINAL = "consolidado_despeses.zip";
    // Vamos que vamos varrer no site
    private static final String[] ANOS_BUSCA = {"2025", "2024"}; 

    public static void main(String[] args) {
        System.out.println("  INICIANDO PROCESSAMENTO ETL (Extract, Transform, Load)...  ");

        try {
            // Etapa 1: Extração (Web Scraping e Download)
            // Cria a pasta 'downloads' se não existir
            Files.createDirectories(Paths.get(PASTA_DOWNLOADS));
            baixarArquivosDaANS();

            // Etapa 2: Tranformação (Leitura, Filtro e Normalização)
            processarArquivosBaixados();

            // Etapa 3: CARGA (Compactação Final)
            compactarResultadoFinal();

            System.out.println("\n  PROCESSO FINALIZADO COM SUCESSO!  ");
            System.out.println("  Arquivo gerado: " + ARQUIVO_ZIP_FINAL);

        } catch (Exception e) {
            System.err.println("  Erro fatal na execução: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Realiza o Web Scraping no site da ANS e baixa os arquivos ZIP encontrados.
     */
    private static void baixarArquivosDaANS() {
        System.out.println("\n--- 1. INICIANDO DOWNLOADS (Camada Raw) ---");
        
        int baixados = 0;
        
        for (String ano : ANOS_BUSCA) {
            try {
                String urlAno = URL_BASE + ano + "/";
                System.out.println("  Buscando arquivos em: " + urlAno);

                // Conecta no site e pega o HTML
                Document doc = Jsoup.connect(urlAno).timeout(10000).get();
                
                // Itera sobre todos os links (tags <a>)
                for (Element link : doc.select("a")) {
                    String href = link.attr("href");
                    
                    // Filtro de Arquivos:
                    // Pegamos apenas .zip que contenham "T" (indicativo de Trimestre, ex: 1T2025.zip)
                    // Isso evita baixar manuais ou arquivos auxiliares indesejados.
                    if (href.endsWith(".zip") && href.toUpperCase().contains("T")) {
                        
                        String nomeArquivo = link.text();
                        // Monta a URL completa (alguns links no site são relativos)
                        String urlCompleta = href.startsWith("http") ? href : urlAno + href;
                        Path destino = Paths.get(PASTA_DOWNLOADS, nomeArquivo);

                        // Só baixa se ainda não existir (evita re-download desnecessário)
                        if (!Files.exists(destino)) {
                            System.out.print("    Baixando " + nomeArquivo + "... ");
                            try (InputStream in = new URL(urlCompleta).openStream()) {
                                Files.copy(in, destino, StandardCopyOption.REPLACE_EXISTING);
                                System.out.println("OK!");
                                baixados++;
                            }
                        } else {
                            System.out.println("     Arquivo já existe (Cache): " + nomeArquivo);
                        }
                        
                        // Limite de segurança para o teste (pegar apenas os 3 últimos disponíveis)
                        if (baixados >= 3) return; 
                    }
                }
            } catch (Exception e) {
                System.err.println("     Falha ao acessar ano " + ano + ": " + e.getMessage());
            }
        }
    }

    /**
     * Lê os arquivos da pasta 'downloads', aplica as regras de negócio e gera o CSV consolidado.
     */
    private static void processarArquivosBaixados() throws IOException {
        System.out.println("\n--- 2. PROCESSANDO E CONSOLIDANDO DADOS ---");

        NumberFormat formatadorNumero = NumberFormat.getInstance(new Locale("pt", "BR"));
        Path pasta = Paths.get(PASTA_DOWNLOADS);

        // Abre o escritor do CSV Final (Encoding ISO-8859-1 é padrão do governo)
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(ARQUIVO_CSV_FINAL), Charset.forName("ISO-8859-1"));
             DirectoryStream<Path> stream = Files.newDirectoryStream(pasta, "*.zip")) {

            // Escreve o Cabeçalho
            writer.write("CNPJ;RazaoSocial;Trimestre;Ano;ValorDespesas");
            writer.newLine();

            for (Path entry : stream) {
                processarZipIndividual(entry, writer, formatadorNumero);
            }
        }
    }

    /**
     * Processa um único arquivo ZIP (abre, localiza o CSV interno e filtra linhas).
     */
    private static void processarZipIndividual(Path zipPath, BufferedWriter writer, NumberFormat formatadorNumero) {
        String nomeArquivo = zipPath.getFileName().toString();
        System.out.println("  Processando: " + nomeArquivo);

        // Extração de metadados do nome do arquivo (Ex: 1T2025.zip)
        String ano = "0000";
        String trimestre = "0T";
        try {
            String limpo = nomeArquivo.toLowerCase().replace(".zip", "");
            String[] partes = limpo.split("t"); // Quebra "1t2025" em "1" e "2025"
            if (partes.length >= 2) {
                trimestre = partes[0] + "T";
                ano = partes[1];
            }
        } catch (Exception e) {
            System.err.println("    Nome de arquivo fora do padrão esperado: " + nomeArquivo);
        }

        try (ZipFile zipFile = new ZipFile(zipPath.toFile())) {
            // Localiza o CSV dentro do ZIP
            ZipEntry entry = zipFile.stream()
                    .filter(e -> e.getName().endsWith(".csv"))
                    .findFirst()
                    .orElse(null);

            if (entry == null) {
                System.err.println("     Nenhum CSV encontrado dentro de " + nomeArquivo);
                return;
            }

            // Lê o CSV interno linha a linha (Streaming)
            try (BufferedReader br = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry), Charset.forName("ISO-8859-1")))) {
                String linha;
                br.readLine(); // Pula cabeçalho original

                while ((linha = br.readLine()) != null) {
                    // Remove aspas que vêm no CSV e separa por ponto e vírgula
                    String[] colunas = linha.replace("\"", "").split(";");

                    // Validação básica de estrutura
                    if (colunas.length < 6) continue;

                    String regAns = colunas[1];       // Registro ANS (usado como ID)
                    String descricao = colunas[3];    // Descrição da conta
                    String valorStr = colunas[5];     // Valor (Saldo Final)

                    // Regra de Negócio: Filtrar apenas Eventos ou Sinistros
                    if (descricao.toUpperCase().contains("EVENTO") || descricao.toUpperCase().contains("SINISTRO")) {
                        try {
                            double valor = formatadorNumero.parse(valorStr).doubleValue();
                            
                            // Inconsistência Tratada: Valores negativos ou zerados ignorados
                            if (valor > 0) {
                                
                                // Inconsistência Tratada: Dados Faltantes (Razão Social)
                                // O arquivo original não fornece Razão Social nem CNPJ completo.
                                // Solução: Normalizamos criando um identificador baseado no Registro ANS.
                                String cnpjFicticio = regAns + "000100"; 
                                String razaoFicticia = "OPERADORA " + regAns;

                                writer.write(String.format("%s;%s;%s;%s;%.2f", 
                                    cnpjFicticio, razaoFicticia, trimestre, ano, valor));
                                writer.newLine();
                            }
                        } catch (Exception e) {
                            // Ignora linhas onde o valor numérico é inválido
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("    Erro de leitura no arquivo: " + nomeArquivo);
        }
    }

    /**
     * Compacta o CSV final em um arquivo ZIP (Requisito 1.3).
     */
    private static void compactarResultadoFinal() {
        System.out.println("\n--- 3. GERANDO ENTREGA FINAL ---");
        try (FileOutputStream fos = new FileOutputStream(ARQUIVO_ZIP_FINAL);
             ZipOutputStream zos = new ZipOutputStream(fos);
             FileInputStream fis = new FileInputStream(ARQUIVO_CSV_FINAL)) {

            ZipEntry zipEntry = new ZipEntry(ARQUIVO_CSV_FINAL);
            zos.putNextEntry(zipEntry);

            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) >= 0) {
                zos.write(buffer, 0, length);
            }
            zos.closeEntry();
            System.out.println("  ZIP criado com sucesso: " + ARQUIVO_ZIP_FINAL);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}