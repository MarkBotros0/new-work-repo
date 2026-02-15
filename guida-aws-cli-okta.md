# Guida: Configurazione AWS CLI con Okta SAML su Windows

## Prerequisiti

- Windows 10/11
- Python 3.x installato
- Accesso a Okta aziendale con permessi AWS

---

## Step 1: Verificare Python

Python potrebbe essere installato ma non funzionare a causa degli alias di Windows.

```powershell
# Verifica con py launcher (funziona sempre)
py --version

# Verifica pip
py -m pip --version
```

Se `python --version` non funziona ma `py --version` sì, l'alias di Windows sta interferendo.

### Soluzione: Disabilitare alias Windows

1. Apri **Impostazioni** → **App** → **Impostazioni app avanzate** → **Alias di esecuzione delle app**
2. Disabilita **python.exe** e **python3.exe** (quelli del Microsoft Store)

---

## Step 2: Installare gimme-aws-creds

```powershell
py -m pip install gimme-aws-creds
```

### Creare wrapper per evitare problemi con alias

Crea il file `C:\Users\<USERNAME>\AppData\Local\Programs\Python\Python311\Scripts\gimme-creds.cmd`:

```batch
@echo off
py -c "from gimme_aws_creds.main import GimmeAWSCreds; GimmeAWSCreds().run()" %*
```

---

## Step 3: Configurare gimme-aws-creds

Crea il file `C:\Users\<USERNAME>\.okta_aws_login_config`:

```ini
[DEFAULT]
okta_org_url = https://deloittecloud.okta.com
okta_auth_server =
client_id =
gimme_creds_server = appurl
aws_appname =
aws_rolename = all
write_aws_creds = True
cred_profile = default
okta_username =
app_url = https://deloittecloud.okta.com/app/amazon_aws/exk1ev5rjg5VWQeph1d8/sso/saml
resolve_aws_alias = True
include_path = False
preferred_mfa_type = push
remember_device = True
aws_default_duration = 3600
device_token =
force_classic = True
```

**Note:**
- `okta_username`: lascia vuoto, verrà chiesto al login
- `preferred_mfa_type`: `push` per Okta Verify, `token:software:totp` per codice
- `force_classic = True`: necessario per alcune configurazioni Okta

---

## Step 4: Configurare AWS CLI

Crea/modifica `C:\Users\<USERNAME>\.aws\config`:

```ini
[default]
region = eu-central-1
output = json
```

**IMPORTANTE:** La regione deve avere il trattino: `eu-central-1` (non `eu-central1`)

---

## Step 5: Ottenere le credenziali

```powershell
gimme-creds
```

Verrà chiesto:
1. **Username Okta** (email aziendale)
2. **Password Okta**
3. **MFA** (push notification o codice TOTP)
4. **Ruolo AWS** (se hai accesso a più ruoli)

Le credenziali vengono salvate automaticamente in `~/.aws/credentials`.

---

## Step 6: Verificare il funzionamento

```powershell
# Verifica identità
aws sts get-caller-identity

# Lista bucket S3
aws s3 ls

# Lista App Runner services
aws apprunner list-services

# Lista con regione specifica
aws apprunner list-services --region eu-west-1
```

---

## Uso quotidiano

### Login giornaliero

Le credenziali SAML scadono (tipicamente 1-12 ore). Per rinnovarle:

```powershell
gimme-creds
```

### Profili multipli

Se hai accesso a più account AWS, aggiungi profili in `.okta_aws_login_config`:

```ini
[prod]
okta_org_url = https://deloittecloud.okta.com
app_url = https://deloittecloud.okta.com/app/amazon_aws/exk1ev5rjg5VWQeph1d8/sso/saml
aws_rolename = arn:aws:iam::123456789012:role/AdminRole
cred_profile = prod
```

Poi usa:
```powershell
gimme-creds --profile prod
aws s3 ls --profile prod
```

---

## Troubleshooting

| Errore | Causa | Soluzione |
|--------|-------|-----------|
| `Python non trovato` | Alias Windows | Disabilita alias o usa `py` |
| `Could not connect to endpoint URL` | Regione mancante/errata | Controlla `~/.aws/config`, usa `eu-central-1` |
| `No valid SAML assertion` | Sessione scaduta | Esegui di nuovo `gimme-creds` |
| `MFA timeout` | Push non ricevuto | Prova `token:software:totp` nel config |

---

## File di riferimento

| File | Scopo |
|------|-------|
| `~/.okta_aws_login_config` | Configurazione Okta/SAML |
| `~/.aws/config` | Regione e output format |
| `~/.aws/credentials` | Credenziali temporanee (auto-generate) |

---

## Comandi utili

```powershell
# Verifica identità corrente
aws sts get-caller-identity

# Lista servizi per regione
aws apprunner list-services --region eu-central-1
aws apprunner list-services --region eu-west-1

# Rinnova credenziali senza prompt (se remember_device=True)
gimme-creds --action-register-device
```
