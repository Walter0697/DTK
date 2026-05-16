# Terraform module inputs for a shared platform service.
# The repeated structure is intentionally noisy so DTK can trim it down to the
# fields that matter most during review.

variable "app_name" {
  type        = string
  default     = "atlas-platform"
  description = "Primary application name used in release metadata, dashboards, alert routing, and environment-specific naming."
  nullable    = false
}

variable "environment" {
  type        = string
  default     = "production"
  description = "Deployment environment label used to control rollout behavior, operator expectations, and downstream service banners."
  nullable    = false
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "Cloud region for the module. This value is repeated across pipelines, deployment notes, and runbooks."
  nullable    = false
}

variable "instance_type" {
  type        = string
  default     = "m7i.large"
  description = "Compute size for the service tier. The exact value is mostly operational noise until capacity planning is required."
  nullable    = false
}

variable "desired_capacity" {
  type        = number
  default     = 4
  description = "Target number of instances to keep online during normal operation, after safety margins and warm-up behavior are applied."
  nullable    = false
}

variable "min_capacity" {
  type        = number
  default     = 2
  description = "Lower bound used by autoscaling so the service stays available even when demand drops below the normal baseline."
  nullable    = false
}

variable "max_capacity" {
  type        = number
  default     = 12
  description = "Upper bound used by autoscaling so the module cannot overprovision beyond the approved budget envelope."
  nullable    = false
}

variable "log_retention_days" {
  type        = number
  default     = 30
  description = "How long operational logs should remain in storage before automated cleanup removes them."
  nullable    = false
}

variable "feature_flag_bundle" {
  type        = string
  default     = "core"
  description = "Feature bundle selector used to gate rollout-specific behavior, usually only changed during coordinated launches."
  nullable    = false
}

variable "support_email" {
  type        = string
  default     = "platform@example.com"
  description = "Contact address for platform escalations, incident triage, and customer-facing handoff notes."
  nullable    = false
}
