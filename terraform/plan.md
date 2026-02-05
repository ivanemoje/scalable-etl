.
├── modules/                # Reusable code (S3, Glue, IAM)
│   ├── s3/
│   │   ├── main.tf
│   │   └── variables.tf
│   └── glue/
│       ├── main.tf
│       └── variables.tf
└── environments/           # Environment-specific configurations
    └── prod/               # Your production folder
        ├── main.tf         # Calls modules or defines prod resources
        ├── variables.tf    # Environment variables (instance sizes, etc.)
        ├── outputs.tf      # Results (Bucket ARNs, Job IDs)
        ├── terraform.tfvars # Actual values for variables
        └── providers.tf    # AWS provider and backend config