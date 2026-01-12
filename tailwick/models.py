from flask_login import UserMixin
from . import db

class User(db.Model, UserMixin):
    __tablename__ = "users"

    # Primary Key
    user_id = db.Column("user_id", db.Integer, primary_key=True)

    # Other fields (must match exactly as in MySQL)
    role_id = db.Column("RoleId", db.Integer, nullable=True)
    user_name = db.Column("user_name", db.String(150), unique=True, nullable=False)
    user_contact = db.Column("user_contact", db.String(20), nullable=True)
    user_address = db.Column("user_address", db.String(255), nullable=True)
    email = db.Column("Email", db.String(150), unique=False, nullable=True)
    gender = db.Column("Gender", db.String(10), nullable=True)
    payment_status = db.Column("Paymentstatus", db.String(50), nullable=True)
    login_password = db.Column("Loginpassword", db.String(255), nullable=False)

    # ✅ Match naming with your login route
    trnM_AssociationId = db.Column("AssociationId", db.Integer, nullable=True)
    subgroup_id = db.Column("SubGroupId", db.Integer, nullable=True)

    def get_id(self):
        """Flask-Login requires this: must return primary key as str."""
        return str(self.user_id)

    def __repr__(self):
        return f"<User id={self.user_id} username={self.user_name}>"
