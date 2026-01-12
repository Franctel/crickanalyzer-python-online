import bcrypt
from tailwick import create_app, db
from tailwick.models import User

# Create Flask app context so SQLAlchemy works
app = create_app()
app.app_context().push()

def create_test_user(
    username="testuser",
    password="test123",
    email="test@example.com",
    assoc_id=1,
    role_id=2,  # Default RoleId (2 = Normal User, adjust if needed)
    subgroup_id=1
):
    print("🔑 Creating test user...")
    print(f"   Username = {username}")
    print(f"   Password (plain) = {password}")

    # Hash the password
    hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    print(f"   Password (bcrypt hash) = {hashed_pw}")

    # Check if user already exists
    existing = User.query.filter_by(user_name=username).first()
    if existing:
        print(f"⚠️ User '{username}' already exists in DB (id={existing.user_id}). Skipping insert.")
        return

    # Create new user (all mandatory fields filled!)
    new_user = User(
        role_id=role_id,
        user_name=username,
        user_contact="9999999999",
        user_address="Test Address",
        email=email,
        gender="Male",                # ✅ required
        payment_status="UnPaid",      # ✅ required
        login_password=hashed_pw,     # ✅ required
        association_id=assoc_id,
        subgroup_id=subgroup_id
    )

    db.session.add(new_user)
    db.session.commit()
    print(f"✅ Test user '{username}' created successfully with id={new_user.user_id}")

if __name__ == "__main__":
    create_test_user()
