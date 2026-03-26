
# Public key
resource "aws_key_pair" "supplychain360_key" {
  key_name   = "supplychain360_key"
  public_key = var.public_key_path
}

# Create ec2 instance
resource "aws_instance" "supplychain360_ec2" {
  ami           = "ami-0c17cb8e234335014"
  instance_type = var.instance_type

  key_name = aws_key_pair.supplychain360_key.key_name

  subnet_id = aws_subnet.public_subnet.id

  associate_public_ip_address = true

  vpc_security_group_ids = [aws_security_group.supplychain360_sg.id]

  tags = {
    Name = "supplyChain360-instance"
  }
}