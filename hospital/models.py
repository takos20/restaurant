from django.contrib.auth.models import AbstractUser
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.forms import ValidationError
from django.utils import timezone
import datetime
from multiselectfield import MultiSelectField
from django.contrib.postgres.fields import JSONField
from django.utils.timezone import now
from django.contrib.auth.models import Group, Permission
import uuid
from django.db.models import Q
USER_CHOICES = [
    ('RESPONSIBLE', 'Responsible'),
    ('ADMIN', 'Admin'),
    ('DOCTOR', 'Doctor'),
    ('PATIENT', 'Patient'), ('CASHIER', 'Cashier'),
    ('RECEPTIONIST', 'Receptionist'),
    ('HR', 'HR')
]
TYPE_USER = [
    ('NORMAL', 'Normal'),
    ('DOCTOR', 'Doctor'),
    ('PATIENT', 'Patient'), 
]

TYPE_ACCOUNT = [
    ('PRIVATE', 'PRIVATE'),
    ('INSURED', 'INSURED'),
    ('PREPAID', 'PREPAID'), 
    ('CORPORATE', 'CORPORATE'),
    ('SOCIAL', 'SOCIAL'),
    ('DEPENDENT', 'DEPENDENT'), 
]
DAYS_CHOICES = [
    ('MONDAY', 'MONDAY'),
    ('TUESDAY', 'TUESDAY'),
    ('WEDNESDAY', 'WEDNESDAY'),
    ('FRIDAY', 'FRIDAY'),
    ('THURSDAY', 'THURSDAY'),
    ('SATURDAY', 'SATURDAY'),
    ('SUNDAY', 'SUNDAY')
]
SYMPTOMS_CHOICES = [
    ('REDNESS', 'REDNESS'),
    ('PAIN', 'PAIN'),
    ('DECREASED_VISUAL_ACUITY', 'DECREASED_VISUAL_ACUITY'),
    ('LTCH', 'LTCH'),
    ('SECRETIONS', 'SECRETIONS')
]
TYPE_MVT = [
    ('ENTRY', 'Entry'), ('TRANSFER', 'Transfer'),
    ('EXIT', 'Exit')
]
status_occupation = [
    ('PENDING', 'PENDING'),
    ('FINISHED', 'FINISHED')
]
status_room = [
    ('NOT_OCCUPIED', 'NOT_OCCUPIED'),
    ('OCCUPIED', 'OCCUPIED'),
    ('IN_PREPARATION', 'IN_PREPARATION')
]

TYPE_ENTERPRISE = [
    ('HOSPITAL', 'HOSPITAL'),
    ('RETAIL', 'RETAIL')
]

type_accommodation = [
    ('BOOKING', 'BOOKING'),
    ('OCCUPATION', 'OCCUPATION')
]
type_anomaly = [
    ('MISSING_PRODUCTS', 'MISSING_PRODUCTS'),
    ('EXPIRED_PRODUCTS', 'EXPIRED_PRODUCTS'),
    ('DAMAGED_PRODUCTS', 'DAMAGED_PRODUCTS'),
    ('POOR_QUALITY_PRODUCTS', 'POOR_QUALITY_PRODUCTS'),
    ('OTHER_ANOMALY', 'OTHER_ANOMALY'),
    ('EXCESS_PRODUCTS', 'EXCESS_PRODUCTS')
]
type_product = [
    ('CONSUMABLE', 'CONSUMABLE'),
    ('REAGENT', 'REAGENT')
]
type_diagnostic = [
    ('RIGHT_EYE', 'RIGHT_EYE'),
    ('LEFT_EYE', 'LEFT_EYE')
]
TYPE_CERTIFICATE = [
    ('MEDICAL_CERTIFICATE', 'MEDICAL_CERTIFICATE'), ('SPORTS_SKILLS', 'SPORTS_SKILLS'),
    ('STOPPAGE_WORK', 'STOPPAGE_WORK'), ('STOPPAGE_SCHOOL', 'STOPPAGE_SCHOOL'),
    ('CHRONIC_DESEASE', 'CHRONIC_DESEASE')
]
BILL_CHOICES = [
    ('YES', 'Yes'),
    ('NO', 'No')
]
TYPE_STOPPAGE = [
    ('EXTENSION', 'EXTENSION'),
    ('GETTING_BACK', 'GETTING_BACK'),
    ('STOP', 'STOP')
]
TYPE_ACTION = [
    ('REMOVE', 'Retire'),
    ('ADD', 'Ajoute'),
]
BILLS_CHOICES = [
    ('ONSITE', 'onsite'),
    ('DELIVERY', 'delivery'),
    ('CATERING', 'catering'),
    ('EVENT', 'event'),
]
BILL_SHAPE_CHOICES = [
    ('ORDINANCE', 'Ordinance'),
    ('CLASSIC', 'Classic')
]
TYPE_CASH_MOVEMENT = [
    ('CREDIT', 'CREDIT'),
    ('DEBIT', 'DEBIT')
]
PAYEMENT_STATUS = [
    ('PAID', 'PAID'),
    ('PARTIAL', 'PARTIAL')
]
OVERPAYMENT_ACTION = [
    ('PREPAID', 'PREPAID'),
    ('CASH', 'CASH')
]
TYPE_CASH = [
    ('CASH_COUNTERS', 'CASH_COUNTERS'),
    ('CASH_MAIN', 'CASH_MAIN'),
    ('CASH_BANK', 'CASH_BANK')
]

POSITION_CHOICES = [
    ('LONGITUDINAL', 'LONGITUDINAL'),
    ('OBLIQUE', 'OBLIQUE'),
    ('TRANSVERSE', 'TRANSVERSE')
]
STATUS_CHOICES = [
    ('INTERNAL', 'INTERNAL'),
    ('EXTERNAL', 'EXTERNAL')
]
TYPE_CHOICES = [
    ('GENERAL', 'GENERAL'),
    ('CONSULTANT', 'CONSULTANT'),
    ('SPECIALIST', 'SPECIALIST')
]
TYPE_CONSULT = [
    ('DENTAL', 'DENTAL'),
    ('MEDICAL', 'MEDICAL'),
    ('PRENATAL', 'PRENATAL'),
    ('OPHTALMOLOGIC', 'OPHTALMOLOGIC')
]
PATIENT_SHAPE = [
    ('NORMAL', 'NORMAL'),
    ('OLD', 'OLD'),
    ('CHILD', 'CHILD'),
    ('PREGNANT', 'PREGNANT'),
    ('DISABILITY', 'DISABILITY')
]
APPOINTMENT_TYPE = [
    ('GENERAL', 'GENERAL'),
    ('SPECIALIST', 'SPECIALIST'),
    ('CONSULTANT', 'CONSULTANT')
]
TYPE_PLANNING = [
    ('RDV', 'RDV'),
    ('SURGICAL_INTERVENTIONS', 'SURGICAL_INTERVENTIONS')
]
APPOINTMENT_STATUS = [
    ('NOT_STARTED', 'NOT_STARTED'),
    ('PARAMETER_TAKING', 'PARAMETER_TAKING'),
    ('WAITING', 'WAITING'),
    ('PENDING', 'PENDING'),
    ('FINISHED', 'FINISHED'),
    ('CANCELED', 'CANCELED')
]
CONSULTATION_STATUS = [
    ('WAITING', 'WAITING'),
    ('PENDING', 'PENDING'),
    ('FINISHED', 'FINISHED')
]

CAT_ANT_CHOICES = [
    ('MEDICAL', 'Medical'),
    ('SURGICAL', 'Surgical'),
    ('FAMILY', 'Family'),
    ('DRUG INTOLERANCE', 'Drug intolerance'),
    ('VACCINATION', 'Vaccination'),
    ('MENTION PART', 'Mention part')
]
PATIENT_SHAPE_CHOICES = [
    ('EMPLOYEE', 'EMPLOYEE'),
    ('INDIGENT', 'INDIGENT'),
    ('NORMAL', 'NORMAL'),
    ('COVERED', 'COVERED')
]

TYPE_STATUS = [
    ('SINGLE', 'Single'),
    ('MARRIED', 'Married'),
    ('DIVORCE', 'Divorce'),
    ('WIDOW', 'Widow'),
]

class SyncBaseModel(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4, null=True, blank=True)
    sync_version = models.BigIntegerField(default=1)
    createdAt = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updatedAt = models.DateTimeField(auto_now=True, null=True, blank=True)
    deleted = models.BooleanField(default=False)
    deletedAt = models.DateTimeField(auto_now_add=True, null=True, blank=True)

    def save(self, *args, **kwargs):
        if self.pk:
            self.sync_version += 1
        super().save(*args, **kwargs)

    class Meta:
        abstract = True

def add_region():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Region.objects.order_by('-id').first()
    codeUpdate = "RGN000001"
    if get_last is None:
        return codeUpdate
    else:
        if get_last:
            return generate_next_code(digit=6,last_code=get_last.code, prefix='RGN')
        else:
            assign_missing_city_codes_bulk(model=Region, digit=6, prefix='RGN')
        
def add_code(self, prefix, digit, model, hospital=None):
    last = model.objects.exclude(code__isnull=True).exclude(code='').order_by('-id').first()
    last_code = last.code if last else None

    number = 1
    if last_code:
        match = re.search(r'\d+', last_code)
        if match:
            number = int(match.group()) + 1

    self.code = f"{prefix}{number:0{digit}d}"

class Hospital(SyncBaseModel):
    
    price_hospitalization = models.IntegerField(default=0)
    rules_reduction = models.JSONField(default=list, null=True, blank=True)
    # depart = models.ManyToManyField(Departments, related_name='departments')
    name = models.CharField(max_length=255, null=True, blank=True)
    stock_min_peremption = models.CharField(max_length=255, null=True, blank=True)
    consultation_time = models.CharField(max_length=255, null=True, blank=True)
    days_before_expiry_date = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=255, null=True, blank=True)    
    address_ip = models.CharField(max_length=255, null=True, blank=True)    
    start_day_work = models.CharField(max_length=255, null=True, blank=True)
    end_day_work = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=255, null=True, blank=True)
    logo = models.FileField(upload_to='%Y/%m/%d/', null=True, blank=True)
    zip_code = models.CharField(max_length=255, null=True, blank=True)
    slogan = models.CharField(max_length=255, null=True, blank=True)
    taxpayer = models.CharField(max_length=255, null=True, blank=True)
    email = models.EmailField(max_length=1255, blank=True, null=True)
    type_enterprise = models.CharField(max_length=255, choices=TYPE_ENTERPRISE, null=True, blank=True)
    # taux de TVA collectée concerne les ventes et est facturée aux clients avant d’être reversée à l’État
    VAT_collected = models.FloatField(default=0.0, null=True, blank=True)
    #VAT_to_pay La TVA à payer est dès lors la différence entre la TVA collectée et la TVA déductible
    #La TVA déductible est apparentée aux achats que l’entreprise effectue auprès de ses différents fournisseurs.
    deductible_VAT = models.FloatField(default=0.0, null=True, blank=True)
    is_inventory = models.BooleanField(default=False)
    use_delivery = models.BooleanField(default=False)

    class Meta:
        db_table = 'hospital'
        ordering = ('-createdAt',)


class SyncQueueItem(models.Model):
    model_name = models.CharField(max_length=200)
    hospital_id = models.IntegerField(null=True, blank=True)
    operation = models.CharField(max_length=10)  # CREATE / UPDATE
    data = models.JSONField()
    sync_version = models.IntegerField(default=1)
    status = models.CharField(max_length=20, default="PENDING")  # PENDING / PROCESSING / SUCCESS / FAILED
    processedAt = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'sync_queue'
        ordering = ('-id',)

class Region(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_region)
    name = models.CharField(max_length=255, null=True, blank=True)
    
    class Meta:
        db_table = 'region'
        ordering = ('-id',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "RGN",
            digit = 6, model=Region)
            
        super().save(*args, **kwargs)


def generate_next_code(digit, last_code, prefix):
    regex = re.compile(r'\d+')
    numbers = re.findall(regex, last_code) if last_code else []
    number = int(numbers[0]) + 1 if numbers else 1
    return f"{prefix}{number:0{digit}d}"

def assign_missing_city_codes_bulk(model, digit, prefix):
    objs = list(model.objects.filter(code__isnull=True) | model.objects.filter(code=''))

    last_obj = model.objects.exclude(code__isnull=True).exclude(code='').order_by('-id').first()
    last_code = last_obj.code if last_obj else None
    current_code = last_code

    for obj in objs:
        current_code = generate_next_code(digit=digit, last_code=current_code, prefix=prefix)
        obj.code = current_code

    model.objects.bulk_update(objs, ['code'])

def add_city():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = City.objects.order_by('-id').first()
    prefix = 'CTY'
    digit = 6
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=City, digit=digit, prefix=prefix)



class City(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_city)
    region = models.ForeignKey(Region, on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = 'city'
        ordering = ('-id',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "CTY",
            digit = 6, model=City)
            
        super().save(*args, **kwargs)
def add_district():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = District.objects.order_by('-id').first()
    prefix = 'DTT'
    digit = 6
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=District, digit=digit, prefix=prefix)


class District(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    is_default = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_district)
    city = models.ForeignKey(City, on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = 'district'
        ordering = ('-id',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "DTT",
            digit = 6, model=District)
            
        super().save(*args, **kwargs)



def add_type_patient():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Type_patient.objects.order_by('-id').first()
    prefix = 'TYP'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Type_patient, digit=digit, prefix=prefix)


class Type_patient(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_type_patient, unique=True)
    title = models.CharField(max_length=255, null=True, blank=True)
    is_default = models.BooleanField(default=False)

    class Meta:
        db_table = 'type_patient'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "TYP",
            digit = 4, model=Type_patient)
            
        super().save(*args, **kwargs)


# Create your models here.
def add_expenses_nature():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Expenses_nature.objects.order_by('-id').first()
    prefix = 'NAT'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Expenses_nature, digit=digit, prefix=prefix)


class Expenses_nature(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_expenses_nature, unique=True)
    type = models.CharField(max_length=255, choices=TYPE_MVT, null=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    account_number = models.CharField(max_length=255, null=True, blank=True)
    is_active = models.BooleanField(default=False)

    class Meta:
        
        db_table = 'expenses_nature'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "NAT",
            digit = 4, model=Expenses_nature)
            
        super().save(*args, **kwargs)


import re

def add_category():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Category.objects.order_by("-id").first()
    prefix = 'CAT'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Category, digit=digit, prefix=prefix)


class Category(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True)
    name_language = models.JSONField(default=list, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    billable = models.CharField(max_length=255, choices=BILL_CHOICES, null=True, blank=True)

    class Meta:
        db_table = 'categories'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "CAT",
            digit = 4, model=Category)
            
        super().save(*args, **kwargs)

def add_insurance():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Insurance.objects.order_by('-id').first()
    prefix = 'ENT'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Insurance, digit=digit, prefix=prefix)


class Insurance(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_insurance, unique=True)
    is_active = models.BooleanField(default=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    number = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=255, null=True, blank=True)
    location = models.CharField(max_length=255, null=True, blank=True)
    mailbox = models.CharField(max_length=255, null=True, blank=True)
    percent = models.FloatField(default=0.0, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    responsible = models.CharField(max_length=255, null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    email = models.EmailField(max_length=1255, blank=True, null=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'insurance'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "ENT",
            digit = 4, model=Insurance)
            
        super().save(*args, **kwargs)

def add_patient():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Patient.objects.order_by('-id').first()
    prefix = 'PAT'
    digit = 6
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Patient, digit=digit, prefix=prefix)


class Patient(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, blank=True, db_index=True)
    dateNaiss = models.CharField(max_length=255, blank=True)
    phone = models.CharField(max_length=255, blank=True, db_index=True)
    number = models.CharField(max_length=255, blank=True)
    primary_insured = models.CharField(max_length=255, blank=True)
    child = models.CharField(max_length=255, blank=True)
    date_id = models.CharField(max_length=255, blank=True)
    number_id = models.CharField(max_length=255, blank=True)
    maritalStatus = models.CharField(max_length=255, choices=TYPE_STATUS, blank=True, null=True)
    emergency_contact = models.CharField(max_length=255, blank=True)
    emergency_name = models.CharField(max_length=255, blank=True)
    city = models.ForeignKey(City, on_delete=models.CASCADE, null=True)
    mother_name = models.CharField(max_length=255, blank=True)
    district = models.CharField(max_length=255, blank=True)
    type_id = models.CharField(max_length=255, blank=True)
    religion = models.CharField(max_length=255, blank=True)
    pathologies = models.CharField(max_length=255, blank=True)
    allergies = models.CharField(max_length=255, blank=True)
    blood_group = models.CharField(max_length=255, blank=True)
    weight = models.CharField(max_length=255, blank=True)
    electrophoresis = models.CharField(max_length=255, blank=True)
    size = models.CharField(max_length=255, blank=True)
    bpm = models.CharField(max_length=255, blank=True)
    temperature = models.CharField(max_length=255, blank=True)
    padiasto = models.CharField(max_length=255, blank=True)
    pasysto = models.CharField(max_length=255, blank=True)
    gender = models.CharField(max_length=255, blank=True)
    other_phone = models.CharField(max_length=255, blank=True)
    age = models.CharField(max_length=255, blank=True)
    scale_price = models.FloatField(default=0.0, null=True, blank=True)
    is_assured = models.BooleanField(default=False, null=True)
    is_default = models.BooleanField(default=False, null=True)
    insurance = models.ForeignKey(Insurance, on_delete=models.CASCADE, null=True)
    type_patient = models.ForeignKey(Type_patient, on_delete=models.CASCADE, null=True)
    # shape = models.CharField(choices=PATIENT_SHAPE_CHOICES, max_length=255)
    address = models.CharField(max_length=255, blank=True)
    code = models.CharField(max_length=255, blank=True, default=add_patient, unique=True)
    email = models.EmailField(max_length=255, blank=True, null=True)

    @property
    def balance(self):
        total_amount=self.bills.aggregate(total_amount=Sum('bills__amount_net'))['total_amount'] or 0
        total_insurance=self.bills.aggregate(total_insurance=Sum('bills__insurance'))['total_insurance'] or 0
        total_insurance_patient=self.bills.aggregate(total_insurance_patient=Sum('bills__insurance_patient'))['total_insurance_patient'] or 0
        total_paye=self.bills.aggregate(total_paye=Sum('bills__amount_paid'))['total_paye'] or 0
        total={'total_amount':total_amount,'total_insurance':total_insurance,'total_insurance_patient':total_insurance_patient,'total_paye':total_paye}
        return total
    class Meta:
        db_table = 'patient'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "CLI",
            digit = 6, model=Patient)
            
        super().save(*args, **kwargs)


class User(AbstractUser):
    # code = models.CharField(max_length=255, null=False, default=add_user, unique=True)
    # patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True)
    hospital = models.ForeignKey(Hospital, on_delete=models.SET_NULL, null=True)
    username = models.CharField(max_length=255, unique=True, null=False)
    patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True)
    is_active = models.BooleanField(default=True, db_column='is_active')
    # is_actived = models.BooleanField(default=True, db_column='is_active')
    is_online = models.BooleanField(default=True, db_column='is_online')
    role = models.CharField(choices=USER_CHOICES, max_length=255)
    type = models.CharField(choices=TYPE_USER, max_length=255 , null=True)
    uuid = models.UUIDField(default=uuid.uuid4, null=True, editable=False)
    sync_version = models.BigIntegerField(default=1)
    createdAt = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updatedAt = models.DateTimeField(auto_now=True, null=True, blank=True)
    deleted = models.BooleanField(default=False)
    deletedAt = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    
    # objects = UserManager()
    def save(self, *args, **kwargs):
        if self.pk:
            self.sync_version += 1
        super().save(*args, **kwargs)

    class Meta:
        db_table = 'users'
        ordering = ('-createdAt',)


def add_cash():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Cash.objects.order_by('-id').first()
    prefix = 'CSH'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Cash, digit=digit, prefix=prefix)


class Cash(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_cash, unique=True)
    cash_fund = models.IntegerField(default=0, null=True)
    balance = models.IntegerField(default=0)
    is_transfer = models.BooleanField(default=False)
    type_cash = models.CharField(max_length=255, choices=TYPE_CASH, null=True, blank=True)
    is_active = models.BooleanField(default=True, db_column='is_active')
    open_date = models.DateTimeField(verbose_name="Open Date", auto_now_add=True, null=True, blank=True)
    # open_date = models.DateTimeField(verbose_name="Open Date", auto_now_add_add=True, null=True, blank=True)
    close_date = models.DateTimeField(verbose_name="Close Date", null=True, blank=True)

    class Meta:
        db_table = 'cash'
        ordering = ('-id',)


    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "CSH",
            digit = 4, model=Cash)
            
        super().save(*args, **kwargs)


class CategoryTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    category = models.ForeignKey(Category, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'category_translation'
        unique_together = ('category', 'language')



class Storage_depots(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    name_language = models.JSONField(default=list, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    is_default = models.BooleanField(default=False)
    
    class Meta:
        db_table = 'storage_depots'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "SDP",
            digit = 4, model=Storage_depots)
            
        super().save(*args, **kwargs)
class WarehouseTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    warehouse = models.ForeignKey(Storage_depots, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'warehouse_translation'
        unique_together = ('warehouse', 'language')
class ExtendedGroup(SyncBaseModel):
    group = models.ForeignKey(Group, on_delete=models.CASCADE, related_name='extension')
    hospital = models.ForeignKey(
        Hospital,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='groups'
    )
    is_active = models.BooleanField(default=True)
    createdBy = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='created_groups'
    )

    def __str__(self):
        return f"{self.group.name}"

    def get_permissions(self):
        return self.group.permissions.all()
    
    class Meta:
        unique_together = ['group', 'hospital']
        db_table = 'extended_group'
        ordering = ('-id',)




class Stock_movement(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True)
    type_movement = models.CharField(max_length=255, choices=TYPE_MVT)
    movement_value = models.FloatField(default=0.0)
    reason_movement = models.CharField(max_length=255, null=True, blank=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    is_valid= models.BooleanField(default=False)
    
    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last = Stock_movement.objects.filter(code__startswith=f"STM-{current_year}") \
            .order_by('-id').first()
        
        if last and last.code:
            try:
                last_number = int(last.code.split('-')[-1])
            except ValueError:
                last_number = 0
        else:
            last_number = 0

        next_number = last_number + 1
        return f"STM-{current_year}-{next_number:04d}"
    class Meta:
        db_table = 'stock_movement'
        ordering = ('-id',)


class Bills(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    district = models.ForeignKey(District, on_delete=models.CASCADE, null=True)

    address = models.TextField(null=True,)
    delivery_fee = models.FloatField(default=0, null=True,)
    delivery_service = models.CharField(max_length=100, null=True, blank=True)  # Glovo, interne
    delivery_man = models.CharField(max_length=100, null=True, blank=True)
    # expected_duration  = models.IntegerField(default=0, null=True, blank=True)
    # expected_time = models.DateTimeField(null=True, blank=True)

    event_name = models.CharField(max_length=150,null=True,)
    organizer = models.CharField(max_length=150,null=True,)
    location = models.TextField(null=True,)
    event_start = models.DateTimeField(null=True,)
    event_end = models.DateTimeField(null=True,)
    estimated_guests = models.IntegerField(null=True,)
    contract_amount = models.FloatField(default=0)

    company_name = models.CharField(max_length=150, null=True,)
    event_date = models.DateField(null=True,)
    event_location = models.TextField(null=True,)
    advance_payment = models.FloatField(default=0)
    balance_due = models.FloatField(default=0)
    contact_person = models.IntegerField(default=0, null=True, blank=True)

    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True, blank=True)
    cash = models.ForeignKey(Cash, on_delete=models.CASCADE, null=True)
    patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True, related_name='bills')
    phone_number = models.IntegerField(default=0, null=True, blank=True)
    bank_card_number = models.IntegerField(default=0, null=True, blank=True)
    transaction_ref_bank_card = models.IntegerField(default=0, null=True, blank=True)
    transaction_ref_om = models.IntegerField(default=0, null=True, blank=True)
    transaction_ref_momo = models.IntegerField(default=0, null=True, blank=True)
    net_payable = models.FloatField(default=0.0, null=True, blank=True)
    bills_amount = models.FloatField(default=0.0, null=True, blank=True)
    amount_bank_card = models.FloatField(default=0.0, null=True, blank=True)
    amount_om = models.FloatField(default=0.0, null=True, blank=True)
    amount_cash = models.FloatField(default=0.0, null=True, blank=True)
    amount_momo = models.FloatField(default=0.0, null=True, blank=True)
    amount_prepaid = models.FloatField(default=0.0, null=True, blank=True)
    tva = models.FloatField(default=0.0, null=True, blank=True)
    ttc = models.FloatField(default=0.0, null=True, blank=True)
    amount_received = models.IntegerField(default=0, null=True, blank=True)
    amount_paid = models.IntegerField(default=0, null=True, blank=True)
    delivery = models.CharField(max_length=255, null=True, blank=True)
    balance = models.FloatField(default=0.0, null=True, blank=True)
    refund = models.FloatField(default=0.0, null=True, blank=True)
    total_amount = models.FloatField(default=0.0, null=True, blank=True)
    additional_info = models.CharField(max_length=255, null=True, blank=True)
    bill_type = models.CharField(choices=BILLS_CHOICES, max_length=255)
    patient_name = models.CharField(max_length=255, null=True, blank=True)
    cash_code = models.CharField(max_length=255, null=True, blank=True)
    cashier_name = models.CharField(max_length=255, null=True, blank=True)
    doctor_name = models.CharField(max_length=255, null=True, blank=True)
    deleted_by = models.CharField(max_length=255, null=True, blank=True)
    code = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=255, choices=PAYEMENT_STATUS, blank=True, null=True)
    overpayment_action = models.CharField(max_length=255, choices=OVERPAYMENT_ACTION, blank=True, null=True)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last_number = 0

        while True:
            last = Bills.objects.filter(code__startswith=f"FAC-{current_year}") \
                                .order_by('-id').first()
            if last and last.code:
                try:
                    last_number = int(last.code.split('-')[-1])
                except ValueError:
                    last_number = 0
            next_number = last_number + 1
            code = f"FAC-{current_year}-{next_number:06d}"

            # Vérifier si le code existe déjà
            if not Bills.objects.filter(code=code).exists():
                return code
            # sinon on incrémente et on recommence
            last_number += 1

    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    class Meta:
        db_table = 'bills'
        ordering = ('-id',)        
class DeliveryInfo(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    bills = models.OneToOneField(Bills, related_name="deliverys", on_delete=models.CASCADE, null=True)
    address = models.TextField()
    delivery_fee = models.FloatField(default=0)
    delivery_service = models.CharField(max_length=100, null=True, blank=True)  # Glovo, interne
    delivery_man = models.CharField(max_length=100, null=True, blank=True)
    expected_duration  = models.IntegerField(default=0, null=True, blank=True)
    expected_time = models.DateTimeField(null=True, blank=True)
    delivered = models.BooleanField(default=False)

    class Meta:
        db_table = "delivery"
        ordering = ('-createdAt',)

class CateringInfo(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    bills = models.OneToOneField(Bills, related_name="catering", on_delete=models.CASCADE, null=True)
    company_name = models.CharField(max_length=150)
    event_date = models.DateField()
    event_location = models.TextField()
    advance_payment = models.FloatField(default=0)
    balance_due = models.FloatField(default=0)
    contact_person = models.CharField(max_length=100)

    class Meta:
        db_table = "catering"
        ordering = ('-createdAt',)

class EventInfo(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    bills = models.OneToOneField(Bills, related_name="event", on_delete=models.CASCADE, null=True)
    event_name = models.CharField(max_length=150)
    organizer = models.CharField(max_length=150)
    location = models.TextField()
    event_start = models.DateTimeField()
    event_end = models.DateTimeField()
    estimated_guests = models.IntegerField()
    contract_amount = models.FloatField(default=0)
    paid = models.BooleanField(default=False)

    class Meta:
        db_table = "event"
        ordering = ('-createdAt',)


def add_ingredient():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Ingredient.objects.order_by("-id").first()
    prefix = 'ING'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Ingredient, digit=digit, prefix=prefix)

class Ingredient(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)    
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    name_language = models.JSONField(default=list, null=True, blank=True)
    unit = models.CharField(max_length=20, default="g")  # g, ml, pcs
    cmup = models.FloatField(default=0.0, null=True, blank=True)
    price_per_unit = models.DecimalField(max_digits=12, decimal_places=1, default=0, null=True)  # prix par unité
    last_paid_price = models.DecimalField(max_digits=12, decimal_places=1, default=0, null=True)  # dernier prix d'achat
    
    @property
    def quantity_per_depot(self):
        """
        Renvoie les quantités disponibles par dépôt avec le nom du dépôt.
        Chaque dépôt contient `quantity` et `quantity_two`.
        """
        from django.db.models import Sum

        # Récupère les stocks non supprimés pour cet ingrédient
        stocks = (
            Stock.objects.filter(ingredient=self, deleted=False)
            .values('storage_depots__name')
            .annotate(
                total_quantity=Sum('quantity'),
                total_quantity_two=Sum('quantity_two')
            )
        )

        # Construit un dictionnaire {nom_depot: {quantity, quantity_two}}
        return {
            stock['storage_depots__name']: {
                'quantity': stock['total_quantity'] or 0,
                'quantity_two': stock['total_quantity_two'] or 0
            }
            for stock in stocks
        }

    @property
    def total_quantity(self):
        """
        Quantité totale de cet ingrédient sur tous les dépôts (somme de quantity)
        """
        return sum([v['quantity'] for v in self.quantity_per_depot.values()])

    @property
    def total_quantity_two(self):
        """
        Quantité totale de cet ingrédient sur tous les dépôts (somme de quantity_two)
        """
        return sum([v['quantity_two'] for v in self.quantity_per_depot.values()])
    class Meta:
        db_table = 'ingredient'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "ING",
            digit = 4, model=Ingredient)
            
        super().save(*args, **kwargs)

class IngredientTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    ingredient = models.ForeignKey(Ingredient, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'ingredient_translation'
        unique_together = ('ingredient', 'language')


def add_dish():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Dish.objects.order_by("-id").first()
    prefix = 'DIS'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Dish, digit=digit, prefix=prefix)

# Plat principal
class Dish(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    name_language = models.JSONField(default=list, null=True, blank=True)
    category = models.ForeignKey(Category, related_name="products", on_delete=models.CASCADE)
    price = models.FloatField(default=0)  # prix de vente
    preparation_time = models.IntegerField(default=0)  # en minutes
    is_active = models.BooleanField(default=True)
    is_delivery = models.BooleanField(default=True, null=True)

    @property
    def cost(self):
        """Coût total des ingrédients"""
        total = sum([ri.cost for ri in self.ingredients.all()])
        return total

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "DIS",
            digit = 4, model=Dish)
            
        super().save(*args, **kwargs)
    class Meta:
        db_table = 'dish'
        ordering = ('-id',)

class DishTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    dish = models.ForeignKey(Dish, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    class Meta:
        db_table = 'dish_translation'
        unique_together = ('dish', 'language')

from decimal import Decimal

class Recipes(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    dish = models.OneToOneField(Dish, on_delete=models.CASCADE, null=True)
    total_amount = models.FloatField(default=0.0, null=True, blank=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    
    class Meta:
        db_table = 'recipes'
        ordering = ('-createdAt',)


class ComposeIngredient(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    name_language = models.JSONField(default=list, null=True, blank=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    unit = models.CharField(max_length=20, default="g")  # g, ml, pcs
    stock_quantity = models.FloatField(default=0, null=True,)  # quantité en unité
    price_per_unit = models.DecimalField(max_digits=12, decimal_places=1, default=0, null=True)
    total_amount = models.FloatField(default=0.0, null=True, blank=True)
    
    class Meta:
        db_table = 'compose_ingredient'
        ordering = ('-createdAt',)

class ComposeIngredientTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    compose_ingredient = models.ForeignKey(ComposeIngredient, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'compose_ingredient_translation'
        unique_together = ('compose_ingredient', 'language')



# Historique de préparation / consommation
class DishPreparation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    # ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    # compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, null=True)
    quantity = models.IntegerField(default=1)  # nombre de portions préparées

    def prepare(self):
        """
        Décrémente le stock des ingrédients utilisés.
        """
        for ri in self.dish.recipe_ingredients.all():
            total_needed = ri.quantity * self.quantity
            # if ri.ingredient.stock_quantity < total_needed:
            #     raise ValueError(f"Stock insuffisant pour {ri.ingredient.name}")
            ri.ingredient.stock_quantity -= total_needed
            ri.ingredient.save()

    @property
    def total_cost(self):
        """Coût total de la préparation"""
        return sum([ri.cost * self.quantity for ri in self.dish.recipe_ingredients.all()])
    
    class Meta:
        db_table = 'dish_preparation'
        ordering = ('-createdAt',)

class Stock(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True, related_name="stocks")
    compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True, related_name="stocks")
    quantity = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))
    quantity_two = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))

    class Meta:
        db_table = 'stock'
        ordering = ('-id',)
        constraints = [
        models.UniqueConstraint(
            fields=['hospital', 'storage_depots', 'ingredient'],
            condition=Q(ingredient__isnull=False, deleted=False),
            name='unique_stock_ingredient'
        ),
        models.UniqueConstraint(
            fields=['hospital', 'storage_depots', 'compose_ingredient'],
            condition=Q(compose_ingredient__isnull=False, deleted=False),
            name='unique_stock_compose'
        ),
        models.CheckConstraint(
            check=(
                Q(ingredient__isnull=False, compose_ingredient__isnull=True) |
                Q(ingredient__isnull=True, compose_ingredient__isnull=False)
            ),
            name='only_one_stock_type'
        ),
    ]

# Historique de préparation / consommation
class ComposePreparation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    stock_quantity = models.FloatField(default=0, null=True,)  # nombre de portions préparées
    total_amount = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))
    class Meta:
        db_table = 'compose_preparation'
        ordering = ('-createdAt',)

class ComposePreparationTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    compose_preparation = models.ForeignKey(ComposePreparation, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'compose_preparation_translation'
        unique_together = ('compose_preparation', 'language')

class DetailsComposePreparation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, related_name='composes', null=True)
    compose_preparation = models.ForeignKey(ComposePreparation, on_delete=models.CASCADE, related_name='compose_preparations', null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))
    cost = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))
    class Meta:
        db_table = 'details_compose_preparation'
        ordering = ('-createdAt',)
    #     constraints = [

    #     # Unicité ingrédient simple
    #     models.UniqueConstraint(
    #         fields=['hospital', 'compose_preparation', 'ingredient'],
    #         condition=Q(ingredient__isnull=False, deleted=False),
    #         name='unique_compose_preparation_ingredient'
    #     ),

    #     # Unicité ingrédient composé
    #     models.UniqueConstraint(
    #         fields=['hospital', 'compose_preparation', 'compose_ingredient'],
    #         condition=Q(compose_ingredient__isnull=False, deleted=False),
    #         name='unique_compose_preparation_compose'
    #     ),

    #     # Empêcher les deux champs remplis ou vides
    #     models.CheckConstraint(
    #         check=(
    #             Q(ingredient__isnull=False, compose_ingredient__isnull=True) |
    #             Q(ingredient__isnull=True, compose_ingredient__isnull=False)
    #         ),
    #         name='only_one_ingredient'
    #     ),
    # ]

# Ingrédients par plat (recette)
class RecipeIngredient(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    recipes = models.ForeignKey(Recipes, on_delete=models.CASCADE, related_name="recipe_ingredients", null=True)
    dish = models.ForeignKey(Dish,  related_name="ingredients", on_delete=models.CASCADE, null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    # compose_preparation = models.ForeignKey(ComposePreparation, on_delete=models.CASCADE, null=True)
    compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, null=True)
    quantity = models.FloatField()  # quantité par portion

    @property
    def cost(self):
        """Coût de cet ingrédient pour ce plat"""
        if self.ingredient:
            if self.ingredient.price_per_unit is None:
                return Decimal(0)
            else:
                return Decimal(self.quantity) * self.ingredient.price_per_unit
        elif self.compose_ingredient:
            if self.compose_ingredient.price_per_unit is None:
                return Decimal(0)
            else:
                return Decimal(self.quantity) * self.compose_ingredient.price_per_unit
        return Decimal(0)

    # def __str__(self):
    #     return f"{self.dish.name} - {self.ingredient.name}"
    
    class Meta:
        db_table = 'recipe_ingredient'
        ordering = ('-createdAt',)
        constraints = [
            models.UniqueConstraint(
                fields=['recipes', 'dish', 'ingredient'],
                condition=Q(ingredient__isnull=False),
                name='unique_recipe_dish_ingredient'
            ),
            models.UniqueConstraint(
                fields=['recipes', 'dish', 'compose_ingredient'],
                condition=Q(compose_ingredient__isnull=False),
                name='unique_recipe_dish_compose'
            ),
            models.CheckConstraint(
                check=(
                    Q(ingredient__isnull=False, compose_ingredient__isnull=True) |
                    Q(ingredient__isnull=True, compose_ingredient__isnull=False)
                ),
                name='only_one_ingredient_type'
            ),
        ]

class DetailsComposeIngredient(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    # hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    compose_ingredient = models.ForeignKey(ComposeIngredient, related_name="compose_ingredients", on_delete=models.CASCADE, null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    quantity = models.FloatField()  # quantité par portion
    @property
    def cost(self):
        """Coût de cet ingrédient pour ce plat"""
        if self.ingredient.price_per_unit is None:
            return Decimal(0)
        return Decimal(self.quantity) * self.ingredient.price_per_unit

    
    class Meta:
        unique_together = ('compose_ingredient','ingredient')
        db_table = 'deyails_compose_ingredient'
        ordering = ('-createdAt',)

class Promotion(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    name_language = models.JSONField(default=list, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    start_date = models.DateTimeField()
    end_date = models.DateTimeField()
    priority = models.IntegerField(default=0)  # ordre d’application
    cumulative = models.BooleanField(default=False)

    class Meta:
        db_table = 'promotion'
        ordering = ('-id',)

class DetailsBills(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    cash = models.ForeignKey(Cash, on_delete=models.CASCADE, null=True, blank=True)
    bills = models.ForeignKey(Bills, on_delete=models.CASCADE, null=True, blank=True, related_name='bills')
    dish = models.ForeignKey(Dish, on_delete=models.CASCADE, null=True, blank=True)
    quantity_served = models.IntegerField(default=0)
    patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True, related_name='details_bills')
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    quantity_ordered = models.IntegerField(default=0)
    stock_initial = models.IntegerField(default=0)
    tva = models.FloatField(default=0.0, null=True, blank=True)
    amount_gross = models.FloatField(default=0.0, null=True, blank=True)
    amount_net = models.FloatField(default=0.0, null=True, blank=True)
    cost_production = models.FloatField(default=0.0, null=True, blank=True)
    pun = models.FloatField(default=0.0, null=True, blank=True)
    pub = models.FloatField(default=0.0, null=True, blank=True)
    delivery = models.CharField(max_length=255, null=True, blank=True)
    promotion = models.ForeignKey(Promotion, on_delete=models.CASCADE, null=True, blank=True)
    margin = models.FloatField(default=0.0, null=True, blank=True)
    
    class Meta:
        db_table = 'details_bills'
        ordering = ('id',)


def add_suppliers():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = Suppliers.objects.order_by('-id').first()
    prefix = 'FOU'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=Suppliers, digit=digit, prefix=prefix)


class Suppliers(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=255, null=True, blank=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_suppliers, unique=True)
    mailbox = models.CharField(max_length=255, null=True, blank=True)
    city = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=255, null=True, blank=True)
    fax = models.CharField(max_length=255, null=True, blank=True)
    taxpayer_number = models.CharField(max_length=255, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    name_representative = models.CharField(max_length=255, null=True, blank=True)
    phone_representative = models.CharField(max_length=255, null=True, blank=True)
    is_active = models.BooleanField(default=False)
    is_default = models.BooleanField(default=False)
    
    class Meta:
        db_table = 'suppliers'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "FOU",
            digit = 4, model=Suppliers)
            
        super().save(*args, **kwargs)


class Inventory(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    reason_inventory = models.CharField(max_length=255, null=True, blank=True)
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    
    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last = Inventory.objects.filter(code__startswith=f"INV-{current_year}") \
            .order_by('-id').first()
        
        if last and last.code:
            try:
                last_number = int(last.code.split('-')[-1])
            except ValueError:
                last_number = 0
        else:
            last_number = 0

        next_number = last_number + 1
        return f"INV-{current_year}-{next_number:04d}"

    class Meta:
        db_table = 'inventory'
        ordering = ('-id',)



class Purchase_order(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    suppliers = models.ForeignKey(Suppliers, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    date_received = models.CharField(max_length=255, null=True, blank=True)
    delivery_number = models.CharField(max_length=255, null=True, blank=True)
    purchase_amount = models.IntegerField(default=0)
    is_received = models.BooleanField(default=False)
    
    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last = Purchase_order.objects.filter(code__startswith=f"PCO-{current_year}") \
            .order_by('-id').first()
        
        if last and last.code:
            try:
                last_number = int(last.code.split('-')[-1])
            except ValueError:
                last_number = 0
        else:
            last_number = 0

        next_number = last_number + 1
        return f"PCO-{current_year}-{next_number:04d}"
    class Meta:
        db_table = 'purchase_order'
        ordering = ('-id',)


class Supplies(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    suppliers = models.ForeignKey(Suppliers, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    additional_info = models.CharField(max_length=255, null=True, blank=True)
    reference_no = models.CharField(max_length=255, null=True, blank=True)
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True)
    supply_amount = models.IntegerField(default=0)
    is_accounted = models.BooleanField(default=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    
    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last = Supplies.objects.filter(code__startswith=f"SUP-{current_year}") \
            .order_by('-id').first()
        
        if last and last.code:
            try:
                last_number = int(last.code.split('-')[-1])
            except ValueError:
                last_number = 0
        else:
            last_number = 0

        next_number = last_number + 1
        return f"SUP-{current_year}-{next_number:04d}"

    class Meta:
        db_table = 'supplies'
        ordering = ('-id',)

class DetailsStock_movement(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True)
    storage_depots_dest = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True, blank=True,
                                            related_name='storage_depots_dest')
    stock_movement = models.ForeignKey(Stock_movement, on_delete=models.CASCADE, related_name='stock_movements', null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    total_amount = models.FloatField(default=0.0, null=True, blank=True)
    quantity = models.IntegerField(default=0)
    stock_initial = models.IntegerField(default=0)
    unit_price = models.FloatField(default=0.0, null=True, blank=True)
    type_movement = models.CharField(max_length=255, choices=TYPE_MVT, blank=True)
    
    class Meta:
        unique_together = ('stock_movement','ingredient')
        db_table = 'details_stock_movement'
        ordering = ('-createdAt',)

class DetailsInventory(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    storage_depots = models.ForeignKey(Storage_depots, on_delete=models.CASCADE, null=True)
    inventory = models.ForeignKey(Inventory, on_delete=models.CASCADE, null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    amount = models.FloatField(default=0.0, null=True, blank=True)
    amount_adjusted = models.FloatField(default=0.0, null=True, blank=True)
    cmup = models.FloatField(default=0.0, null=True, blank=True)
    quantity_stock = models.IntegerField(default=0)
    quantity_adjusted = models.IntegerField(default=0)

    class Meta:
        unique_together = ('inventory','ingredient')
        db_table = 'details_inventory'
        ordering = ('-createdAt',)


class Cash_movement(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    cash = models.ForeignKey(Cash, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    type = models.CharField(max_length=255, choices=TYPE_MVT, null=True, blank=True )
    type_cash_movement = models.CharField(max_length=255, choices=TYPE_CASH_MOVEMENT, null=True, blank=True)
    cash_origin = models.ForeignKey(Cash, on_delete=models.CASCADE, null=True, related_name='origin')
    cash_destination = models.ForeignKey(Cash, on_delete=models.CASCADE, null=True, related_name='destination')
    balance_before = models.IntegerField(default=0)
    balance_after = models.IntegerField(default=0)
    physical_amount = models.IntegerField(default=0)
    # name = models.CharField(max_length=255, null=True, blank=True)
    motive = models.CharField(max_length=255, null=True, blank=True)
    expenses_nature = models.ForeignKey(Expenses_nature, on_delete=models.CASCADE, null=True, related_name='expense_cash')
    amount_movement = models.IntegerField(default=0)
    difference = models.IntegerField(default=0)
    
    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last = Cash_movement.objects.filter(code__startswith=f"CMV-{current_year}") \
            .order_by('-id').first()
        
        if last and last.code:
            try:
                last_number = int(last.code.split('-')[-1])
            except ValueError:
                last_number = 0
        else:
            last_number = 0

        next_number = last_number + 1
        return f"CMV-{current_year}-{next_number:04d}"

    class Meta:
        db_table = 'cash_movement'
        ordering = ('-createdAt',)


def add_patient_account():
    ''' Returns the next default value for the `tens` field'''
    # Retrieve a list of `YourModel` instances, sort them by
    # the `tens` field and get the largest entry

    get_last = PatientAccount.objects.order_by('-id').first()
    prefix = 'PAC'
    digit = 4
    if get_last:
        return generate_next_code(digit=digit,last_code=get_last.code, prefix=prefix)
    else:
        assign_missing_city_codes_bulk(model=PatientAccount, digit=digit, prefix=prefix)

class PatientAccount(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    code = models.CharField(max_length=255, null=True, blank=True, default=add_patient_account, unique=True)
    patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True, related_name='account_patient')
    balance = models.FloatField(default=0.0, null=True, blank=True)
    unpaid = models.FloatField(default=0.0, null=True, blank=True)
    type_account = models.CharField(choices=TYPE_ACCOUNT, max_length=255, null=True, blank=True, default='PRIVATE')
    
    class Meta:
        db_table = 'patient_account'
        ordering = ('-createdAt',)

    def save(self, *args, **kwargs):
        if not self.code:
            # Générer le prochain code unique
            add_code(self, prefix = "PAC",
            digit = 4, model=PatientAccount)
            
        super().save(*args, **kwargs)
class DetailsBillsIngredient(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    is_treated = models.BooleanField(default=False, null=True)
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    details_bills = models.ForeignKey(DetailsBills, on_delete=models.CASCADE, related_name='options', null=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True,)
    compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, null=True,)
    # compose_preparation = models.ForeignKey(ComposePreparation, on_delete=models.CASCADE, null=True,)
    action = models.CharField(max_length=255, choices=TYPE_ACTION, null=True, blank=True )
    quantity = models.FloatField(default=0.0, null=True, blank=True)
    total_amount = models.FloatField(default=0.0, null=True, blank=True)
    impact_price = models.FloatField(default=0.0, null=True, blank=True)
    
    class Meta:
        db_table = 'details_bills_ingredient'
        ordering = ('-createdAt',)



class DetailsPatientAccount(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    patient_account = models.ForeignKey(PatientAccount, on_delete=models.CASCADE, null=True)
    balance_before = models.FloatField(default=0.0, null=True, blank=True)
    balance = models.FloatField(default=0.0, null=True, blank=True)
    balance_after = models.FloatField(default=0.0, null=True, blank=True)
    unpaid_before = models.FloatField(default=0.0, null=True, blank=True)
    unpaid = models.FloatField(default=0.0, null=True, blank=True)
    unpaid_after = models.FloatField(default=0.0, null=True, blank=True)
    type_operation = models.CharField(max_length=255, choices=TYPE_CASH_MOVEMENT, null=True, blank=True)
    source = models.CharField(max_length=50,null=True,)  # FACTURE, INVENTAIRE, PERTE...
    reference_id = models.IntegerField(null=True)
    comment = models.CharField(max_length=5555, null=True, blank=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['type_operation', 'hospital', 'source']),
            models.Index(fields=['reference_id']),
            models.Index(fields=['-createdAt']),  # Pour order_by
        ]
        db_table = 'details_patient_account'
        ordering = ('id',)


class PatientSettlement(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    cash = models.ForeignKey(Cash, on_delete=models.CASCADE, null=True)
    patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True)
    bills = models.ForeignKey(Bills, on_delete=models.CASCADE, null=True, blank=True)
    amount_received = models.IntegerField(default=0, null=True, blank=True)
    amount_paid = models.IntegerField(default=0, null=True, blank=True)
    refund = models.FloatField(default=0.0, null=True, blank=True)
    current_balance = models.FloatField(default=0.0, null=True, blank=True)
    new_balance = models.FloatField(default=0.0, null=True, blank=True)
    wordings = models.CharField(max_length=255, null=True, blank=True)
    payment_method = models.CharField(max_length=255, null=True, blank=True)
    code = models.CharField(max_length=255, null=True, blank=True, unique=True)
    overpayment_action = models.CharField(max_length=255, choices=OVERPAYMENT_ACTION, blank=True, null=True)
    phone_number = models.IntegerField(default=0, null=True, blank=True)
    bank_card_number = models.IntegerField(default=0, null=True, blank=True)
    transaction_ref_bank_card = models.IntegerField(default=0, null=True, blank=True)
    transaction_ref_om = models.IntegerField(default=0, null=True, blank=True)
    transaction_ref_momo = models.IntegerField(default=0, null=True, blank=True)
    amount_bank_card = models.FloatField(default=0.0, null=True, blank=True)
    amount_om = models.FloatField(default=0.0, null=True, blank=True)
    amount_cash = models.FloatField(default=0.0, null=True, blank=True)
    amount_momo = models.FloatField(default=0.0, null=True, blank=True)
    amount_prepaid = models.FloatField(default=0.0, null=True, blank=True)


    def save(self, *args, **kwargs):
        if not self.code:
            self.code = self._generate_code()
        super().save(*args, **kwargs)

    def _generate_code(self):
        current_year = datetime.date.today().year
        last = PatientSettlement.objects.filter(code__startswith=f"RGP-{current_year}") \
            .order_by('-id').first()
        
        if last and last.code:
            try:
                last_number = int(last.code.split('-')[-1])
            except ValueError:
                last_number = 0
        else:
            last_number = 0

        next_number = last_number + 1
        return f"RGP-{current_year}-{next_number:06d}"

    class Meta:
        db_table = 'patient_settlement'
        ordering = ('-id',)

# class Module(SyncBaseModel):
#     is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
#     hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
#     contentType = models.ManyToManyField(ContentType, blank=True)
#     # patient = models.ForeignKey(Patient, on_delete=models.CASCADE, null=True)
#     name = models.CharField(max_length=255, null=True, blank=True)
    
    
    
    
#     deletedAt = models.DateTimeField(null=True, blank=True)

#     # objects = UserManager()

#     class Meta:
#         db_table = 'module'
#         ordering = ('-createdAt',)


class Archive(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    file = models.FileField(upload_to='%Y/%m/%d/', null=True, blank=True)
    number_bills = models.IntegerField(default=0, null=True, blank=True)
    year = models.CharField(max_length=255, null=True, blank=True)
    month = models.CharField(max_length=255, null=True, blank=True)
    
    class Meta:
        db_table = 'archive'
        ordering = ('-createdAt',)


class BackupFile(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    file = models.FileField(upload_to='%Y/%m/%d/', null=True, blank=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        db_table = 'backup_file'
        ordering = ('-createdAt',)


from django.db.models import Sum
class DetailsSupplies(SyncBaseModel):
    
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    supplies = models.ForeignKey(Supplies, on_delete=models.CASCADE,related_name='supplies', null=True, blank=True)
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))
    quantity_two = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal('0.000'))
    type_product = models.CharField(max_length=255, choices=type_product, null=True, blank=True)
    expiry_date = models.CharField(max_length=255, null=True, blank=True)
    stock_initial = models.IntegerField(default=0,null=True, blank=True)
    total_amount = models.IntegerField(default=0,null=True, blank=True)
    business_unit = models.IntegerField(default=0,null=True, blank=True)
    arrival_price = models.IntegerField(default=0,null=True, blank=True)
    unit_price = models.FloatField(default=0.0,null=True, blank=True)
    cmup = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)  # prix par unité
    class Meta:
        unique_together = ('supplies', 'ingredient')
        db_table = 'details_supplies'
        ordering = ('-createdAt',)

    # def save(self, *args, **kwargs):
    #     super().save(*args, **kwargs)

    #     total = DetailsSupplies.objects.filter(
    #         supplies=self.supplies
    #     ).aggregate(Sum('total_amount'))['total_amount__sum'] or 0

    #     self.supplies.supply_amount = total
    #     self.supplies.save()

class MovementStock(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True) 
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, null=True)
    compose_ingredient = models.ForeignKey(ComposeIngredient, on_delete=models.CASCADE, null=True)
    type = models.CharField(max_length=255, choices=TYPE_MVT, null=True)
    quantity = models.FloatField()
    source = models.CharField(max_length=50)  # FACTURE, INVENTAIRE, PERTE...
    reference_id = models.IntegerField(null=True)
    
    class Meta:
        db_table = 'movement_stock'
        ordering = ('-createdAt',)

from django.contrib.auth.models import Permission

class ExtendedPermission(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures  # Partagé entre structures
    hospital = models.ForeignKey(
        Hospital,
        on_delete=models.CASCADE,
        related_name='permissions'
    )
    permission = models.ForeignKey(
        Permission,
        on_delete=models.CASCADE,
        related_name='extended_permissions'
    )
    is_active = models.BooleanField(default=True)
    granted_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    granted_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True
    )
        
    class Meta:
        db_table = 'extended_permission'
        ordering = ('-id',)
        unique_together = ['hospital', 'permission']
        indexes = [
            models.Index(fields=['hospital', 'is_active']),
        ]
# # inject application_id atribute to the Django Permission model
# if not hasattr(Permission, 'module'):
#     module_id = models.ForeignKey(Module, db_column='module', on_delete=models.CASCADE, blank=True, null=True)
#     module_id.contribute_to_class(Permission, 'module')

# Ingrédient / stock

class PromotionTranslation(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    promotion = models.ForeignKey(Promotion, related_name='translations', on_delete=models.CASCADE, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    language = models.CharField(max_length=5)  # 'fr', 'en'
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'promotion_translation'
        unique_together = ('promotion', 'language')

class PromotionRule(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    promotion = models.ForeignKey(Promotion, on_delete=models.CASCADE, null=True)
    type_patient = models.ForeignKey(Type_patient, on_delete=models.CASCADE, null=True)
    # conditions
    min_quantity = models.IntegerField(null=True, blank=True)
    # modulo = models.IntegerField(null=True, blank=True)  # ex: % 10
    # modulo_equals = models.IntegerField(null=True, blank=True)

    category = models.ForeignKey(Category, on_delete=models.CASCADE, null=True, blank=True)
    dish = models.ForeignKey(Dish, on_delete=models.CASCADE, null=True, blank=True)

    start_hour = models.TimeField(null=True, blank=True)
    end_hour = models.TimeField(null=True, blank=True)

    class Meta:
        db_table = 'promotion_rule'
        ordering = ('-id',)

class PromotionAction(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    promotion = models.ForeignKey(Promotion, on_delete=models.CASCADE)

    ACTIONS = (
        ('PERCENT', 'Pourcentage'),
        ('FREE', 'Gratuit'),
        ('FIXED', 'Montant fixe'),
    )

    action_type = models.CharField(max_length=20, choices=ACTIONS)
    value = models.FloatField()  # 50% ou 100% ou 2000 FCFA
    class Meta:
        db_table = 'promotion_action'
        ordering = ('-id',)

class StructureArticle(SyncBaseModel):
    is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
    hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
    dish = models.ForeignKey(Dish, on_delete=models.CASCADE, null=True, related_name='prices')
    is_active = models.BooleanField(default=True, null=True)  # Partagé entre structures
    price = models.FloatField()  # 50% ou 100% ou 2000 FCFA

    class Meta:
        unique_together=('hospital', 'dish')
        db_table = 'structure_article'
        ordering = ('-id',)
# from track_actions import constants
# class History(SyncBaseModel):
#     """History model to store user actions"""
#     is_shared = models.BooleanField(default=False, null=True)  # Partagé entre structures
#     hospital = models.ForeignKey(Hospital, on_delete=models.CASCADE, null=True)
#     table_name = models.CharField(max_length=255, blank=False, editable=False)
#     user = models.ForeignKey(User, on_delete=models.PROTECT, null=True)
#     created_at = models.DateTimeField(auto_now_add=True, editable=False)
#     instance_id = models.CharField(
#         max_length=255, null=False, blank=False, editable=False
#     )
#     action = models.CharField(
#         max_length=7,
#         null=False,
#         blank=False,
#         choices=constants.track_actions,
#         editable=False,
#     )
#     path = models.CharField(max_length=255, default="", blank=False, editable=False)
#     request_data = models.TextField(default="", editable=False)
#     response_data = models.TextField(default="", editable=False)

# 