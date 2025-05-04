# Data Quality Shield

**Data Quality Shield** é um pipeline de validação e qualidade de dados baseado em PySpark, estruturado sob a arquitetura de dados em camadas (Raw → Bronze → Silver → Gold). O projeto tem como foco garantir que os dados estejam íntegros, auditáveis e prontos para uso analítico, seguindo boas práticas de engenharia de dados.

Este pipeline está preparado para rodar de forma **100% cloud-native** utilizando serviços da **Microsoft Azure**, como:

- **Azure Synapse Analytics** para processamento distribuído (Spark Pools)
- **Azure Blob Storage** como camada de armazenamento
- **Azure Data Factory ou Synapse Pipelines** para orquestração dos fluxos

---

## Objetivo

Assegurar que os dados da camada Gold:

- Estejam de acordo com o schema esperado
- Não contenham registros duplicados em campos-chave
- Possuam campos obrigatórios preenchidos
- Sigam padrões definidos (regex, tipos, etc.)
- Sejam rastreáveis por meio de logs particionados por data

---

## Tecnologias Utilizadas

| Componente            | Função                                     |
|------------------------|---------------------------------------------|
| **PySpark**            | Processamento distribuído e transformações  |
| **Azure Synapse**      | Execução de jobs Spark gerenciados          |
| **Azure Data Factory** | Orquestração de pipelines                   |
| **Azure Blob Storage** | Armazenamento de dados e logs               |
| **DuckDB**             | Camada de verificação leve e ágil (local)   |
| **Apache Airflow**     | Orquestração local ou alternativa à ADF     |
| **Python 3.12**        | Modularização dos scripts de QA             |

---

## Funcionalidades

- Modularização do código para reuso entre diferentes etapas
- Validações genéricas (tipos, nulos, duplicados) e customizáveis
- Geração de alertas com salvamento em logs particionados por data
- Separação clara entre lógica de negócio, transformação e QA
- Design orientado para escalabilidade e rastreabilidade

