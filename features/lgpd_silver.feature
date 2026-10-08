# language: pt
Funcionalidade: Anonimização de Dados Sensíveis (LGPD) na Camada Silver

  Cenario: O pipeline deve converter o ID do Cliente em um Hash irreversível
    Dado que eu tenho um lote de transacoes brutas da camada Bronze
    Quando o processo da camada Silver for executado
    Entao a coluna "id_cliente" não deve existir no resultado
    E uma nova coluna "id_cliente_anonimizado" deve conter um hash criptografado