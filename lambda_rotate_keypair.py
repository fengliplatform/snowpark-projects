import json
import boto3
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend


# handler function
def lambda_handler(event, context):
    payload = event["data"]

    return_pub_key = {}
    return_pub_key["data"] = []

    for row in payload:
        key_name = row[1]

        # Generate key pair
        key = rsa.generate_private_key(
            backend=crypto_default_backend(),
            public_exponent=65537,
            key_size=2048
        )
        private_key = key.private_bytes(
            crypto_serialization.Encoding.PEM,
            crypto_serialization.PrivateFormat.PKCS8,
            crypto_serialization.NoEncryption()
        )
        public_key = key.public_key().public_bytes(
            crypto_serialization.Encoding.PEM,
            crypto_serialization.PublicFormat.SubjectPublicKeyInfo
        )
        public_key_string = "".join(public_key.decode("utf-8").split("\n")[1:-2])

        # Save private key to secret manager
        client = boto3.client("secretsmanager")
        try:
            client.create_secret(
                Name=key_name,
                SecretString=private_key.decode("utf-8"),
                ForceOverwriteReplicaSecret=True
            )
        except client.exceptions.ResourceExistsException:
            client.update_secret(
                SecretId=key_name,
                SecretString=private_key.decode("utf-8"),

            )
            print(private_key.decode("utf-8"))

        return_pub_key["data"].append([row[0],public_key_string])

    return return_pub_key

    #return {
    #    'statusCode': 200,
    #    'body': json.dumps(public_key_string)
    #}
