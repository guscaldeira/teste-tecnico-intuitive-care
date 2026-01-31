# Teste Técnico - Engenharia de Dados (Questão 1)

Este projeto implementa um pipeline de **ETL (Extract, Transform, Load)** automatizado para coleta e consolidação de Demonstrações Contábeis da Agência Nacional de Saúde Suplementar (ANS).

##  Funcionalidades

1.  **Web Scraping Automatizado:** Identifica e baixa os arquivos de dados mais recentes do repositório FTP da ANS.
2.  **Filtragem Inteligente:** Processa apenas arquivos contendo "Despesas com Eventos e Sinistros".
3.  **Consolidação de Dados:** Unifica dados trimestrais em um formato normalizado (CSV).
4.  **Entrega Compactada:** Gera automaticamente o artefato final `.zip`.

##  Como Executar

**Pré-requisitos:**
* Java JDK 11 ou superior.
* Maven.

**Passo a passo:**

1.  Clone o repositório e acesse a pasta do projeto.
2.  Instale as dependências:
    ```bash
    mvn clean install
    ```
3.  Execute a classe principal:
    * Classe: `com.intuitive.Questao1`
    * A execução criará uma pasta `downloads/` (Staging) e gerará o arquivo `consolidado_despeses.zip` na raiz.

---

##  Decisões de Arquitetura e Justificativas (Trade-offs)

Conforme solicitado no requisito 1.2, foi realizada uma análise técnica entre processamento em memória vs. disco.

### Estratégia Escolhida: Armazenamento em Disco (Staging Area)
Optou-se por baixar os arquivos fisicamente para a pasta `downloads/` antes de processá-los, atuando como uma zona de aterrissagem (Staging Area).

**Justificativa Técnica:**
* **Resiliência (Fail-over):** Em caso de falha de rede ou erro no parse de um arquivo específico, não é necessário realizar o download de todo o lote novamente. O estado é persistido.
* **Escalabilidade Horizontal:** O processamento incremental (leitura arquivo a arquivo do disco) evita o consumo excessivo de memória RAM (Heap Space), prevenindo erros de `OutOfMemoryError` caso o volume de dados da ANS aumente drasticamente no futuro.
* **Auditoria e Data Lineage:** Permite a conferência dos arquivos originais (Raw Data) caso haja dúvidas sobre a integridade dos dados processados.

### Tratamento de Inconsistências (Requisito 1.3)
Durante a ingestão dos dados brutos, foram aplicadas as seguintes regras de negócio:
* **Normalização de CSV:** Padronização do separador (ponto e vírgula) e tratamento de encoding para suportar caracteres especiais (pt-BR).
* **Limpeza de Dados:** Registros com formatação inválida ou corrompida são logados e ignorados para não interromper o fluxo do pipeline (estratégia de "Best Effort" com log de erros).

---
**Author:** Gustavo Caldeira