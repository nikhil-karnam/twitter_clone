module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name = "twitter-cluster"
  kubernetes_version = "1.36"
  enable_irsa = false

  endpoint_public_access                   = true
  enable_cluster_creator_admin_permissions = false

  access_entries = {
    laptop = {
      principal_arn = "arn:aws:iam::905765459973:role/managed/AccountFullAccessRole"
      policy_associations = {
        admin = {
          policy_arn   = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    }
    github_actions = {
      principal_arn = "arn:aws:iam::905765459973:user/github-actions"
      policy_associations = {
        admin = {
          policy_arn   = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    }
  }

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = { before_compute = true }
    eks-pod-identity-agent = { before_compute = true }
    aws-ebs-csi-driver     = {}
  }

  eks_managed_node_groups = {
    main = {
      instance_types = ["m7i-flex.large"]
      min_size       = 2
      max_size       = 3
      desired_size   = 3

      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 2
      }

      iam_role_additional_policies = {
        ebs = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
      }
    }
  }
}
