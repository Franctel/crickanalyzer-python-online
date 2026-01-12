import bcrypt

stored_hash = b"$2y$10$/h9vWXd1C52.CK2AE8robemJ2xj31lCkKVjieb0ou2pCKM/LWqsha"  # bytes
candidate = "password123".encode("utf-8")

if bcrypt.checkpw(candidate, stored_hash):
    print("Password matches!")
else:
    print("Invalid password.")