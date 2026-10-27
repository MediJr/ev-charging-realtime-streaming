variable "project_id" {
  description = "Identifiant du projet Google Cloud de Lumio Recharge"
  type        = string
}

variable "region" {
  description = "Région GCP : Paris, pour garder les données en France"
  type        = string
  default     = "europe-west9"
}

variable "environment" {
  description = "Environnement (dev, prod) : suffixe des ressources"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment doit valoir dev ou prod."
  }
}

variable "raw_retention_days" {
  description = "Durée de conservation des mesures brutes (cadrage : 90 jours)"
  type        = number
  default     = 90
}

variable "billing_account_id" {
  description = "Compte de facturation, pour l'alerte de budget. Vide = pas d'alerte."
  type        = string
  default     = ""
}

variable "monthly_budget_eur" {
  description = "Budget mensuel en euros : alerte à 50 %, 90 % et 100 %"
  type        = number
  default     = 50
}
