# app/models/company.py
class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    default_service_level = Column(Float, default=0.95)  # e.g., 0.95 for 95%
