# Create VPC
resource "aws_vpc" "supplychain360_vpc" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# Private subnet
resource "aws_subnet" "private_subnet" {
  vpc_id     = aws_vpc.supplychain360_vpc.id
  cidr_block = "10.0.1.0/24"

  tags = {
    Name = "${var.project_name}-private-subnet"
  }
}

# Public subnet
resource "aws_subnet" "public_subnet" {
  vpc_id                  = aws_vpc.supplychain360_vpc.id
  cidr_block              = "10.0.3.0/24"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-subnet"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "supplychain360_igw" {
  vpc_id = aws_vpc.supplychain360_vpc.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

# Route Table
resource "aws_route_table" "supplychain360_rt" {
  vpc_id = aws_vpc.supplychain360_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.supplychain360_igw.id
  }

  tags = {
    Name = "${var.project_name}-rt"
  }
}

# Associate route table with public subnet
resource "aws_route_table_association" "rt_association_public" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.supplychain360_rt.id
}
