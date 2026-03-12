import datetime
from django.contrib.postgres.fields import JSONField
from django import forms
from rest_framework import serializers

from hospital.models import TYPE_ACTION, CateringInfo, DeliveryInfo, DetailsBillsIngredient, DetailsPatientAccount, DetailsStock_movement, Dish, EventInfo, Ingredient, PatientAccount,District, Insurance, Recipes, Stock_movement, Storage_depots, User, Hospital, Patient, \
    Cash, Expenses_nature, Cash_movement, Category, \
    Supplies, Suppliers, DetailsSupplies, DetailsBills,PatientSettlement, Inventory, DetailsInventory, \
    Region, City,  Type_patient
from django.core.validators import RegexValidator
from django.utils.translation import gettext_lazy as _
# from django.utils.translation import ugettext_lazy as _
from multiselectfield import MultiSelectField
from hospital.models import Bills

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


class UserForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    is_active = forms.BooleanField(initial=True, required=False)
    username = forms.CharField(required=True)
    createdAt = forms.CharField(required=False)
    code = forms.CharField(required=False)
    role = forms.CharField(required=False)
    type = forms.CharField(required=False)
    patient = forms.ModelChoiceField(required=False, queryset=Patient.objects.all())
    password = forms.CharField(required=True, widget=forms.PasswordInput, min_length=5, max_length=255)

    class Meta:
        model = User
        fields = ('sync_version','is_shared','hospital','username', 'role', 'is_active','type', 'password', 'patient', 'deleted', 'code')


class CashForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    code = forms.CharField(required=False)
    is_active = forms.BooleanField(initial=True, required=False)
    cash_fund = forms.IntegerField(required=False)
    type_cash = forms.CharField(required=True)

    class Meta:
        model = Cash
        fields = ('sync_version','is_shared','hospital','code', 'cash_fund','type_cash', 'is_active', 'deleted')


# class ModuleForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
#     is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
#     hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
#     name = forms.CharField(required=False)
#     is_active = forms.BooleanField(initial=True, required=False)

#     class Meta:
#         model = Module
#         fields = '__all__'


# class UserFormDoctor(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    #is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    #hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
#     is_active = forms.BooleanField(initial=True, required=False)
#     username = forms.CharField(required=True)
#     createdAt = forms.CharField(required=False)
#     role = forms.CharField(required=False)
#     password = forms.CharField(required=True, widget=forms.PasswordInput, min_length=5, max_length=255)
#
#     class Meta:
#         model = User
#         fields = ('sync_version','is_shared','hospital','username', 'role', 'is_active', 'password', 'deleted', 'doctor')


# class UserFormPatient(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    #is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    #hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
#     is_active = forms.BooleanField(initial=True, required=False)
#     username = forms.CharField(required=True)
#     createdAt = forms.CharField(required=False)
#     role = forms.CharField(required=False)
#     password = forms.CharField(required=True, widget=forms.PasswordInput, min_length=5, max_length=255)
#
#     class Meta:
#         model = User
#         fields = ('sync_version','is_shared','hospital','username', 'role', 'is_active', 'password', 'deleted', 'patient')


class Expenses_natureForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    code = forms.CharField(required=False)
    name = forms.CharField(required=False)
    nature = forms.CharField(required=False)
    account_number = forms.CharField(required=False)
    type = forms.CharField(required=False)
    is_active = forms.BooleanField(initial=True, required=False)

    class Meta:
        model = Expenses_nature
        fields = ('sync_version','is_shared','hospital','code', 'type', 'name', 'nature', 'account_number', 'is_active')


class Cash_movementForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    code = forms.CharField(required=False)
    motive = forms.CharField(required=False)
    expenses_nature = forms.ModelChoiceField(required=False, queryset=Expenses_nature.objects.all())
    cash_destination = forms.ModelChoiceField(required=False, queryset=Cash.objects.all())
    cash_origin = forms.ModelChoiceField(required=False, queryset=Cash.objects.all())
    amount_movement = forms.IntegerField(required=False)
    difference = forms.IntegerField(required=False)
    physical_amount = forms.IntegerField(required=False)
    type = forms.CharField(required=False)
    type_cash_movement = forms.CharField(required=False)

    class Meta:
        model = Cash_movement
        fields = ('sync_version','is_shared','difference','hospital','code', 'type','type_cash_movement', 'motive', 'physical_amount','amount_movement', 'expenses_nature', 'cash_origin', 'cash_destination')


class PatientAccountForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    patient = forms.ModelChoiceField(required=False, queryset=Patient.objects.all())
    balance = forms.CharField(required=False)
    type_account = forms.CharField(required=False)

    class Meta:
        model = PatientAccount
        fields = ('sync_version','is_shared','hospital',
            'balance', 'type_account', 'patient')

class InsuranceForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    code = forms.CharField(required=False)
    name = forms.CharField(required=False)
    number = forms.CharField(required=False)   
    email = forms.EmailField(required=False) 
    phone = forms.CharField(required=False)    
    location = forms.CharField(required=False)    
    mailbox = forms.CharField(required=False)
    responsible = forms.CharField(required=False)
    createdAt = forms.CharField(required=False)
    percent = forms.CharField(required=False)
    is_active = forms.BooleanField(initial=True, required=False)
    start_date = forms.CharField(required=False, empty_value=None)
    end_date = forms.CharField(required=False, empty_value=None)

    class Meta:
        model = Insurance
        fields = '__all__'

class SuppliersForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    is_default = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    is_active = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    code = forms.CharField(required=False)
    name = forms.CharField(required=False)
    phone = forms.CharField(required=False, min_length=9, max_length=12, validators=[
        RegexValidator('^((6([5-9][0-9]{7})))$',
                       _('Enter a valid phone format'))
    ])
    phone_representative = forms.CharField(required=False, min_length=9, max_length=12, validators=[
        RegexValidator('^((6([5-9][0-9]{7})))$',
                       _('Enter a valid phone format'))
    ])
    mailbox = forms.CharField(required=False)
    city = forms.CharField(required=False)
    country = forms.CharField(required=False)
    fax = forms.CharField(required=False)
    taxpayer_number = forms.CharField(required=False)
    name_representative = forms.CharField(required=False)
    email = forms.CharField(required=False)
    createdAt = forms.CharField(required=False)

    class Meta:
        model = Suppliers
        fields = ('sync_version','is_shared','hospital','is_default','is_active','code', 'name', 'name_representative', 'phone_representative', 'phone', 'mailbox', 'city', 'country', 'fax', 'taxpayer_number', 'email')


class SuppliesForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    storage_depots = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    code = forms.CharField(required=False)
    reference_no = forms.CharField(required=False)
    supply_amount = forms.IntegerField(required=False)
    additional_info = forms.CharField(max_length=255, required=False)
    is_accounted = forms.BooleanField(initial=False, required=False)

    class Meta:
        model = Supplies
        fields = '__all__'
class RecipesForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    dish = forms.ModelChoiceField(required=False, queryset=Dish.objects.all())
    total_amount = forms.FloatField(required=False)

    class Meta:
        model = Recipes
        fields = '__all__'
class Storage_depotsForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    createdAt = forms.CharField(required=False)
    code = forms.CharField(required=False)
    name_language = serializers.JSONField(required=False)
    is_active = forms.BooleanField(initial=True, required=False)
    is_default = forms.BooleanField(initial=False, required=False)

    class Meta:
        model = Storage_depots
        fields = ('sync_version','is_shared','hospital','code', 'name_language', 'is_default', 'is_active')


class InventoryForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    storage_depots = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    code = forms.CharField(required=False)
    reason_inventory = forms.CharField(required=False)

    class Meta:
        model = Inventory
        fields = '__all__'


class BillsForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    district = forms.ModelChoiceField(required=False, queryset=District.objects.all())
    cash = forms.ModelChoiceField(required=False, queryset=Cash.objects.all())
    patient = forms.ModelChoiceField(required=False, queryset=Patient.objects.all())
    storage_depots = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    amount_received = forms.IntegerField(required=False)
    amount_paid = forms.IntegerField(required=False)
    net_payable = forms.CharField(required=False)
    tva = forms.CharField(required=False)
    ttc = forms.CharField(required=False)
    insurance = forms.CharField(required=False)
    insurance_patient = forms.CharField(required=False)
    bills_amount = forms.CharField(required=False)
    balance = forms.CharField(required=False)
    refund = forms.CharField(required=False)
    delivery = forms.CharField(required=False)
    total_amount = forms.CharField(required=False)
    additional_info = forms.CharField(required=False)
    bill_type = forms.CharField(max_length=255, required=False)
    payment_method = forms.CharField(max_length=255, required=False)
    type_account = forms.CharField(max_length=255, required=False)
    type_accommodation = forms.CharField(max_length=255, required=False)
    bill_type_hospitalization = forms.CharField(max_length=255, required=False)
    bill_shape = forms.CharField(max_length=255, required=False)
    patient_type = forms.CharField(max_length=255, required=False)
    amount_om = forms.CharField(required=False)
    amount_prepaid = forms.CharField(required=False)
    amount_momo = forms.CharField(required=False)
    amount_cash = forms.CharField(required=False)
    amount_bank_card = forms.CharField(required=False)
    phone_number = forms.IntegerField(required=False)
    bank_card_number = forms.IntegerField(required=False)
    transaction_ref_bank_card = forms.IntegerField(required=False)
    transaction_ref_om = forms.IntegerField(required=False)
    transaction_ref_momo = forms.IntegerField(required=False)
    patient_name = forms.CharField(max_length=255, required=False)
    cash_code = forms.CharField(max_length=255, required=False)
    cashier_name = forms.CharField(max_length=255, required=False)
    doctor_name = forms.CharField(max_length=255, required=False)
    code = forms.CharField(max_length=255, required=False)
    is_proforma = forms.BooleanField(required=False)
    is_proforma_valid = forms.BooleanField(required=False)

    class Meta:
        model = Bills
        fields = '__all__'

class Type_patientForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    title = forms.CharField(max_length=255, required=True)
    is_default = forms.BooleanField(initial=False, required=False)

    class Meta:
        model = Type_patient
        fields = ('sync_version','is_shared','hospital','title','is_default')

class PatientSettlementForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    cash = forms.ModelChoiceField(required=False, queryset=Cash.objects.all())
    amount_received = forms.IntegerField(required=False)
    amount_paid = forms.IntegerField(required=False)
    current_balance = forms.IntegerField(required=False)
    new_balance = forms.IntegerField(required=False)
    wordings = forms.CharField(required=False)
    payment_method = forms.CharField(required=False)
    refund = forms.CharField(required=False)
    code = forms.CharField(max_length=255, required=False)
    amount_om = forms.CharField(required=False)
    amount_prepaid = forms.CharField(required=False)
    amount_momo = forms.CharField(required=False)
    amount_cash = forms.CharField(required=False)
    amount_bank_card = forms.CharField(required=False)
    phone_number = forms.IntegerField(required=False)
    bank_card_number = forms.IntegerField(required=False)
    transaction_ref_bank_card = forms.IntegerField(required=False)
    transaction_ref_om = forms.IntegerField(required=False)
    transaction_ref_momo = forms.IntegerField(required=False)

    class Meta:
        model = PatientSettlement
        fields = '__all__'
class Stock_movementForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    storage_depots = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    storage_depots_dest = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    code = forms.CharField(required=False)
    type_movement = forms.CharField(required=False)
    reason_movement = forms.CharField(required=False)
    is_valid = forms.CharField(required=False)
    movement_value = forms.CharField(required=False)

    class Meta:
        model = Stock_movement
        fields = ('sync_version','is_shared','hospital',
            'code', 'type_movement', 'reason_movement', 'storage_depots','storage_depots_dest', 'movement_value', 'is_valid')


class DetailsStock_movementForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    total_amount = forms.CharField(required=False)
    stock_movement = forms.ModelChoiceField(required=True, queryset=Stock_movement.objects.all())
    storage_depots = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    storage_depots_dest = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    unit_price = forms.CharField(required=False)
    quantity = forms.IntegerField(required=False)
    stock_initial = forms.IntegerField(required=False)
    type_movement = forms.CharField(required=False)

    class Meta:
        model = DetailsStock_movement
        fields = ('sync_version','is_shared','hospital',
            'total_amount', 'storage_depots','storage_depots_dest','stock_movement', 'ingredient', 'stock_initial', 'type_movement', 'quantity',
            'unit_price')


class DetailsInventoryForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    ingredient = forms.ModelChoiceField(required=False, queryset=Ingredient.objects.all())
    inventory = forms.ModelChoiceField(required=False, queryset=Inventory.objects.all())
    amount = forms.CharField(required=False)
    amount_adjusted = forms.CharField(required=False)
    quantity_stock = forms.CharField(required=False)
    quantity_adjusted = forms.CharField(required=False)
    cmup = forms.CharField(required=False)

    class Meta:
        model = DetailsInventory
        fields = ('sync_version','is_shared','hospital',
            'amount', 'amount_adjusted', 'ingredient','inventory', 'quantity_adjusted', 'quantity_stock',
            'cmup')

class DetailsSuppliesForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    supplies = forms.ModelChoiceField(required=False, queryset=Supplies.objects.all())
    stock_initial = forms.IntegerField(required=False)
    quantity = forms.IntegerField(required=False)
    quantity_two = forms.IntegerField(required=False)
    total_amount = forms.IntegerField(required=False)
    business_unit = forms.IntegerField(required=False)
    arrival_price = forms.IntegerField(required=False)
    cmup = forms.DecimalField(required=False, decimal_places=2, max_digits=12)
    type_product = forms.CharField(required=False)
    ingredient = forms.ModelChoiceField(required=False, queryset=Ingredient.objects.all())

    class Meta:
        model = DetailsSupplies
        fields = '__all__'



class DetailsBillsForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    stock_initial = forms.IntegerField(required=False)
    type_accommodation = forms.CharField(required=False)
    quantity_served = forms.IntegerField(required=False)
    quantity_ordered = forms.IntegerField(required=False)
    insurance = forms.CharField(required=False)
    insurance_patient = forms.CharField(required=False)
    amount_net = forms.CharField(required=False)
    tva = forms.CharField(required=False)
    amount_gross = forms.CharField(required=False)
    share_doctor = forms.CharField(required=False)
    pun = forms.CharField(required=False)
    margin = forms.CharField(required=False)
    pub = forms.CharField(required=False)
    delivery = forms.CharField(required=False)
    user = forms.ModelChoiceField(required=False, queryset=User.objects.all())
    dish = forms.ModelChoiceField(required=False, queryset=Dish.objects.all())
    storage_depots = forms.ModelChoiceField(required=False, queryset=Storage_depots.objects.all())
    patient = forms.ModelChoiceField(required=False, queryset=Patient.objects.all())
    class Meta:
        model = DetailsBills
        fields = '__all__'

class DetailsBillsIngredientForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)
    is_treated = forms.BooleanField(initial=False, required=False)
    hospital = forms.ModelChoiceField(
        required=False,
        queryset=Hospital.objects.all()
    )
    details_bills = forms.ModelChoiceField(
        required=False,
        queryset=DetailsBills.objects.all()
    )
    item_uid = forms.CharField(required=True)
    action = forms.ChoiceField(
        choices=TYPE_ACTION,
        required=False
    )
    quantity = forms.FloatField()
    total_amount = forms.FloatField()
    impact_price = forms.FloatField(required=False)

    class Meta:
        model = DetailsBillsIngredient
        fields = ('sync_version',
            'is_treated',
            'is_shared',
            'hospital',
            'details_bills',
            'item_uid',
            'action',
            'quantity',
            'total_amount',
            'impact_price'
        )

    def save(self, commit=True):
        instance = super().save(commit=False)

        uid = self.cleaned_data['item_uid']  # "ingredient-147"

        model_type, obj_id = uid.split('-')
        obj_id = int(obj_id)

        if model_type == "ingredient":
            instance.ingredient_id = obj_id
            instance.compose_ingredient = None

        elif model_type == "compose":
            instance.compose_ingredient_id = obj_id
            instance.ingredient = None

        if commit:
            instance.save()

        return instance
class DetailsPatientAccountForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    balance = forms.CharField(required=False)
    type_operation = forms.CharField(required=False)
    user = forms.ModelChoiceField(required=False, queryset=User.objects.all())
    patient_account = forms.ModelChoiceField(required=False, queryset=PatientAccount.objects.all())

    class Meta:
        model = DetailsPatientAccount
        fields = ('sync_version','is_shared','hospital','balance', 'user','type_operation','patient_account')


class PatientForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    email = forms.EmailField(required=False)
    phone = forms.CharField(required=False, min_length=9, max_length=12, validators=[
        RegexValidator('^((6([5-9][0-9]{7})))$',
                       _('Enter a valid phone format'))
    ])
    name = forms.CharField(max_length=255, required=True)
    dateNaiss = forms.CharField(max_length=255, required=False)
    child = forms.CharField(max_length=255, required=False)
    date_id = forms.CharField(max_length=255, required=False)
    number_id = forms.CharField(max_length=255, required=False)
    maritalStatus = forms.CharField(max_length=255, required=False)
    emergency_contact = forms.CharField(max_length=255, required=False)
    emergency_name = forms.CharField(max_length=255, required=False)
    mother_name = forms.CharField(max_length=255, required=False)
    type_id = forms.CharField(max_length=255, required=False)
    religion = forms.CharField(max_length=255, required=False)
    pathologies = forms.CharField(max_length=255, required=False)
    allergies = forms.CharField(max_length=255, required=False)
    blood_group = forms.CharField(max_length=255, required=False)
    weight = forms.CharField(max_length=255, required=False)
    electrophoresis = forms.CharField(max_length=255, required=False)
    size = forms.CharField(max_length=255, required=False)
    bpm = forms.CharField(max_length=255, required=False)
    temperature = forms.CharField(max_length=255, required=False)
    padiasto = forms.CharField(max_length=255, required=False)
    number = forms.CharField(max_length=255, required=False)
    primary_insured = forms.CharField(max_length=255, required=False)
    pasysto = forms.CharField(max_length=255, required=False)
    gender = forms.CharField(max_length=255, required=False)
    other_phone = forms.CharField(required=False, min_length=9, max_length=12, validators=[
        RegexValidator('^((6([5-9][0-9]{7})))$',
                       _('Enter a valid phone format'))
    ])
    age = forms.CharField(max_length=255, required=False)
    is_assured = forms.BooleanField(initial=False, required=False)
    is_default = forms.BooleanField(initial=False, required=False)
    district = forms.CharField(max_length=255, required=False)
    address = forms.CharField(max_length=255, required=False)
    code = forms.CharField(max_length=255, required=False)
    type_patient = forms.ModelChoiceField(required=False, queryset=Type_patient.objects.all())
    city = forms.ModelChoiceField(required=False, queryset=City.objects.all())
    insurance = forms.ModelChoiceField(required=False, queryset=Insurance.objects.all())

    class Meta:
        model = Patient
        fields = '__all__'

class HospitalFormRule(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    # depart = forms.CharField(max_length=255, required=False)
    use_delivery = forms.BooleanField(initial=False, required=False)
    price_hospitalization = forms.IntegerField(required=False)
    rules_reduction = serializers.JSONField(required=False)
    stock_min_peremption = forms.CharField(required=False)
    taxpayer = forms.CharField(required=False)
    consultation_time = forms.CharField(required=False)    
    days_before_expiry_date = forms.CharField(required=False)    
    start_day_work = forms.CharField(required=False)
    end_day_work = forms.CharField(required=False)
    address = forms.CharField(required=True)
    deductible_VAT = forms.CharField(required=False)
    type_enterprise = forms.CharField(required=False)
    VAT_collected = forms.CharField(required=False)
    slogan = forms.CharField(required=False)
    zip_code = forms.CharField(required=False)
    logo = forms.FileField(required=False)
    phone = forms.CharField(required=True, min_length=9, max_length=12, validators=[
        RegexValidator('^((6([5-9][0-9]{7})))$',
                       _('Enter a valid phone format'))
    ])

    class Meta:
        model = Hospital
        fields = ('sync_version','name', 'consultation_time','use_delivery', 'rules_reduction', 'deductible_VAT','type_enterprise','days_before_expiry_date','VAT_collected','address','start_day_work', 'end_day_work', 'phone', 'deleted', 'stock_min_peremption', 'logo', 'zip_code',
            'slogan', 'taxpayer', 'email')

class HospitalForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    # depart = forms.CharField(max_length=255, required=False)
    use_delivery = forms.BooleanField(initial=False, required=False)
    price_hospitalization = forms.IntegerField(required=False)
    stock_min_peremption = forms.CharField(required=False)
    taxpayer = forms.CharField(required=False)
    consultation_time = forms.CharField(required=False)    
    days_before_expiry_date = forms.CharField(required=False)    
    start_day_work = forms.CharField(required=False)
    end_day_work = forms.CharField(required=False)
    address = forms.CharField(required=True)
    deductible_VAT = forms.CharField(required=False)
    type_enterprise = forms.CharField(required=False)
    VAT_collected = forms.CharField(required=False)
    slogan = forms.CharField(required=False)
    zip_code = forms.CharField(required=False)
    logo = forms.FileField(required=False)
    phone = forms.CharField(required=True, min_length=9, max_length=12, validators=[
        RegexValidator('^((6([5-9][0-9]{7})))$',
                       _('Enter a valid phone format'))
    ])

    class Meta:
        model = Hospital
        fields = ('sync_version','name', 'consultation_time','use_delivery', 'deductible_VAT','type_enterprise','days_before_expiry_date','VAT_collected','address','start_day_work', 'end_day_work', 'phone', 'deleted', 'stock_min_peremption', 'logo', 'zip_code',
            'slogan', 'taxpayer', 'email')

class CategoryForm(forms.ModelForm):
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    # hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    sync_version = forms.IntegerField(required=False)
    name_language = serializers.JSONField(required=False)
    code = forms.CharField(max_length=255, required=False)
    billable = forms.CharField(max_length=255, required=False)
    is_active = forms.BooleanField(initial=True, required=False)

    class Meta:
        model = Category
        fields = '__all__'


class RegionForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    # hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    name = forms.CharField(max_length=255, required=False)

    class Meta:
        model = Region
        fields = ('sync_version','is_shared','name',)


class CityForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    # hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    region = forms.ModelChoiceField(required=False, queryset=Region.objects.all())
    name = forms.CharField(max_length=255, required=False)

    class Meta:
        model = City
        fields = ('sync_version','is_shared','name', 'region',)

class DistrictForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_default = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    city = forms.ModelChoiceField(required=False, queryset=City.objects.all())
    name = forms.CharField(max_length=255, required=False)

    class Meta:
        model = District
        fields = ('sync_version','is_shared','is_default','hospital','name', 'city',)

class UserFormUpdate(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    is_shared = forms.BooleanField(initial=False, required=False)  # Partagé entre structures
    hospital = forms.ModelChoiceField(required=False, queryset=Hospital.objects.all())
    password = forms.CharField(required=False, widget=forms.PasswordInput, min_length=3, max_length=15)
    is_active = forms.BooleanField(initial=True, required=False)
    username = forms.CharField(required=False)
    role = forms.CharField(required=False)
    type = forms.CharField(required=False)
    patient = forms.ModelChoiceField(required=False, queryset=Patient.objects.all())
    deleted = forms.CharField(required=False)

    class Meta:
        model = User
        fields = ('sync_version','is_shared','hospital','username', 'role', 'is_active', 'type', 'password', 'patient', 'deleted')

    def save(self, commit=True):
        user = super(UserFormUpdate, self).save(commit=False)
        password = self.cleaned_data["password"]
        print(user.id)
        if password:
            user.set_password(password)
        else:
            get_user = User.objects.filter(id=user.id).last()
            user.password = get_user.password
        if commit:
            user.save()
        return user
        


class HospitalUserForm(forms.Form):
    hospitals = forms.ModelMultipleChoiceField(Hospital.objects.filter(deleted=False))



class DeliveryInfoForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    bills = forms.ModelChoiceField(queryset=Bills.objects.all(), required=False)

    address = forms.CharField(widget=forms.Textarea, required=True)
    delivery_fee = forms.CharField(required=False)
    delivery_service = forms.CharField(required=False)
    delivery_man = forms.CharField(required=False)

    expected_duration = forms.CharField(required=False)
    delivered = forms.BooleanField(required=False)

    class Meta:
        model = DeliveryInfo
        fields = ('sync_version',
            'bills',
            'address',
            'delivery_fee',
            'delivery_service',
            'delivery_man',
            'expected_duration',
            'delivered'
        )
class CateringInfoForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    bills = forms.ModelChoiceField(queryset=Bills.objects.all(), required=False)

    company_name = forms.CharField(required=True)
    event_date = forms.CharField(required=True)
    event_location = forms.CharField(widget=forms.Textarea, required=True)

    advance_payment = forms.CharField(required=False)
    balance_due = forms.CharField(required=False)

    contact_person = forms.CharField(required=True)

    class Meta:
        model = CateringInfo
        fields = ('sync_version',
            'bills',
            'company_name',
            'event_date',
            'event_location',
            'advance_payment',
            'balance_due',
            'contact_person'
        )

class EventInfoForm(forms.ModelForm):
    sync_version = forms.IntegerField(required=False)
    bills = forms.ModelChoiceField(queryset=Bills.objects.all(), required=False)

    event_name = forms.CharField(required=True)
    organizer = forms.CharField(required=True)
    location = forms.CharField(widget=forms.Textarea, required=True)

    event_start = forms.CharField(required=True)
    event_end = forms.CharField(required=True)

    estimated_guests = forms.CharField(required=True)
    contract_amount = forms.CharField(required=False)

    paid = forms.BooleanField(required=False)

    class Meta:
        model = EventInfo
        fields = ('sync_version',
            'bills',
            'event_name',
            'organizer',
            'location',
            'event_start',
            'event_end',
            'estimated_guests',
            'contract_amount',
            'paid'
        )
