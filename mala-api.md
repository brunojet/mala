## API para Submeter Versões de Aplicativos Certificados

### Endpoint
`POST /api/v1/apps/submit`

### Descrição
Esta API permite submeter uma nova versão de um aplicativo certificado. O cliente deve enviar o arquivo APK, o modelo do dispositivo certificado e notas da versão. A API retornará o identificador (chave primária) do registro no banco de dados.

### Parâmetros de Entrada

- `file` (multipart/form-data): O arquivo APK a ser submetido.
- `deviceModel` (string): O modelo do dispositivo certificado.
- `notes` (string): Notas da versão do aplicativo.

### Exemplo de Requisição

```bash
curl -X POST http://localhost:5000/api/v1/apps/submit \
  -F "file=@/path/to/your/app.apk" \
  -F "data={\"deviceModel\": \"ModeloXYZ\", \"notes\": \"Notas da versão\"}" \
  -H "Content-Type: multipart/form-data"
```

#### Requisição
```json
{
  "deviceModel": "ModeloXYZ",
  "notes": "Notas da versão"
}
```

#### Resposta
```json
{
  "id": "12345"
}
```

### Especificação OpenAPI
```yaml
openapi: 3.0.0
info:
  title: API para Submeter Versões de Aplicativos Certificados
  version: 1.0.0
paths:
  /api/v1/apps/submit:
    post:
      summary: Submete uma nova versão de um aplicativo certificado
      description: Esta API permite submeter uma nova versão de um aplicativo certificado. O cliente deve enviar o arquivo APK, o modelo do dispositivo certificado e notas da versão. A API retornará o identificador (chave primária) do registro no banco de dados.
      requestBody:
        required: true
        content:
          multipart/form-data:
            schema:
              type: object
              properties:
                file:
                  type: string
                  format: binary
                  description: O arquivo APK a ser submetido.
                data:
                  type: object
                  properties:
                    deviceModel:
                      type: string
                      description: O modelo do dispositivo certificado.
                    notes:
                      type: string
                      description: Notas da versão do aplicativo.
      responses:
        '201':
          description: A submissão foi bem-sucedida e o identificador do registro foi retornado.
          content:
            application/json:
              schema:
                type: object
                properties:
                  id:
                    type: string
                    description: O identificador (chave primária) do registro no banco de dados.
        '400':
          description: A requisição está malformada ou faltam parâmetros obrigatórios.
          content:
            application/json:
              schema:
                type: object
                properties:
                  error:
                    type: string
                    description: Descrição do erro.
        '401':
          description: Falha na autenticação. Token JWT inválido ou ausente.
          content:
            application/json:
              schema:
                type: object
                properties:
                  error:
                    type: string
                    description: Descrição do erro.
        '500':
          description: Ocorreu um erro no servidor ao processar a requisição.
          content:
            application/json:
              schema:
                type: object
                properties:
                  error:
                    type: string
                    description: Descrição do erro.
      security:
        - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
```
