"""
Promote an existing user to admin, or create a new admin user.

Usage:
    python scripts/make_admin.py admin              # promote existing user "admin"
    python scripts/make_admin.py myuser             # promote existing user "myuser"
    python scripts/make_admin.py newadmin --create   # create new admin with prompted password
    python scripts/make_admin.py newadmin --create --password secret123
"""

import argparse
import getpass
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import or_

from database import SessionLocal
from models import User
from auth import hash_password


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote or create an admin user")
    parser.add_argument("username", help="Username to promote or create")
    parser.add_argument("--create", action="store_true",
                        help="Create user if not found")
    parser.add_argument("--password", default=None,
                        help="Password (for --create; prompted if omitted)")
    parser.add_argument("--email", default=None,
                        help="Email (for --create; defaults to <username>@polyarena.local)")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        user = db.query(User).filter(
            or_(User.username == args.username, User.email == args.username)
        ).first()

        if user:
            if user.is_admin:
                print(f"User '{user.username}' (id={user.id}) is already an admin.")
                return

            user.is_admin = True
            db.commit()
            print(f"User '{user.username}' (id={user.id}) promoted to admin.")
        elif args.create:
            password = args.password
            if not password:
                password = getpass.getpass(f"Password for '{args.username}': ")
                if len(password) < 6:
                    print("Error: password must be at least 6 characters.", file=sys.stderr)
                    sys.exit(1)

            email = args.email or f"{args.username}@polyarena.local"

            existing_email = db.query(User).filter(User.email == email).first()
            if existing_email:
                print(f"Error: email '{email}' already in use.", file=sys.stderr)
                sys.exit(1)

            user = User(
                username=args.username,
                email=email,
                hashed_password=hash_password(password),
                is_admin=True,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            print(f"Admin user '{user.username}' created (id={user.id}).")
        else:
            print(f"User '{args.username}' not found. Use --create to create a new admin.", file=sys.stderr)
            sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
