import django_filters

from hospital.models import DetailsBillsIngredient, DetailsStock_movement, ExtendedGroup, DetailsPatientAccount, MovementStock, PatientAccount, District, Insurance, Recipes, Stock, Stock_movement, Storage_depots, Type_patient, User, Hospital, Patient, Expenses_nature, \
    Cash, Cash_movement, Category, DetailsSupplies, Bills, \
    DetailsBills,PatientSettlement, Inventory, DetailsInventory, City, Region, Archive, BackupFile, DeliveryInfo, EventInfo, CateringInfo

from hospital.models import Supplies, Suppliers
from django.utils import timezone
from datetime import datetime, time

class UserFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    username = django_filters.CharFilter(lookup_expr='icontains')
    role = django_filters.CharFilter(field_name='groups__name', lookup_expr='exact')
    patient = django_filters.CharFilter(field_name='patient__id', lookup_expr='exact')
    type = django_filters.CharFilter(lookup_expr='exact')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    p_id = django_filters.NumberFilter(field_name='profile__id', lookup_expr='exact')
    p_position_held = django_filters.CharFilter(field_name='profile__position_held', lookup_expr='icontains')
    p_code = django_filters.CharFilter(field_name='profile__code', lookup_expr='icontains')

    class Meta:
        model = User
        fields = {'id': ['exact']}

class HospitalFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    name = django_filters.CharFilter(lookup_expr='icontains')
    stock_min_peremption = django_filters.CharFilter(lookup_expr=('lte'), )
    user_type = django_filters.CharFilter(lookup_expr='iexact')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    use_delivery = django_filters.BooleanFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    address = django_filters.CharFilter(field_name='address', lookup_expr='icontains')
    consultation_time = django_filters.CharFilter(field_name='consultation_time', lookup_expr='icontains')
    phone = django_filters.CharFilter(field_name='phone', lookup_expr='icontains')    
    start_day_work = django_filters.CharFilter(field_name='start day work', lookup_expr='icontains')
    end_day_work = django_filters.CharFilter(field_name='end day work', lookup_expr='icontains')
    slogan = django_filters.CharFilter(field_name='slogan', lookup_expr='icontains')
    taxpayer = django_filters.CharFilter(field_name='taxpayer', lookup_expr='icontains')

    class Meta:
        model = Hospital
        fields = {'id': ['exact']}


class DetailsSuppliesFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    product_name = django_filters.CharFilter(lookup_expr='iexact')
    product_code = django_filters.CharFilter(lookup_expr='icontains')
    quantity = django_filters.CharFilter(lookup_expr='iexact')
    total_amount = django_filters.CharFilter(lookup_expr='iexact')
    type_product = django_filters.CharFilter(lookup_expr='exact')
    createdAt = django_filters.CharFilter(lookup_expr='iexact')
    arrival_price = django_filters.CharFilter(lookup_expr='iexact')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))
    date_range = django_filters.DateRangeFilter(field_name='createdAt')
    ingredient = django_filters.CharFilter(field_name='ingredient__id', lookup_expr='exact')
    supplies = django_filters.CharFilter(field_name='supplies__id', lookup_expr='exact')

    class Meta:
        model = DetailsSupplies
        fields = {'id': ['exact']}
class DetailsBillsIngredientFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    is_treated = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))
    date_range = django_filters.DateRangeFilter(field_name='createdAt')
    ingredient = django_filters.CharFilter(field_name='ingredient__id', lookup_expr='exact')
    details_bills = django_filters.CharFilter(field_name='details_bills__id', lookup_expr='exact')

    class Meta:
        model = DetailsBillsIngredient
        fields = {'id': ['exact']}


class MovementStockFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    type = django_filters.CharFilter( lookup_expr='exact')
    source = django_filters.CharFilter( lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    start_date = django_filters.DateFilter(method='filter_start_date')
    end_date = django_filters.DateFilter(method='filter_end_date')

    def filter_start_date(self, queryset, name, value):
        start = timezone.make_aware(
            datetime.combine(value, time.min)
        )
        return queryset.filter(createdAt__gte=start)

    def filter_end_date(self, queryset, name, value):
        end = timezone.make_aware(
            datetime.combine(value, time.max)
        )
        return queryset.filter(createdAt__lte=end)
    date_range = django_filters.DateRangeFilter(field_name='createdAt')
    ingredient = django_filters.CharFilter(field_name='ingredient__id', lookup_expr='exact')

    class Meta:
        model = MovementStock
        fields = {'id': ['exact']}
from django.db.models.expressions import RawSQL
from django.db.models import TextField
from django.db.models.functions import Cast
class PatientFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    is_default = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    email = django_filters.CharFilter(lookup_expr='iexact')
    createdAt = django_filters.CharFilter(lookup_expr='iexact')
    updateAt = django_filters.CharFilter(lookup_expr='iexact')
    phone = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(field_name='name', lookup_expr='icontains')
    religion = django_filters.CharFilter(lookup_expr='iexact')
    gender = django_filters.CharFilter(lookup_expr='iexact')
    other_phone = django_filters.CharFilter(lookup_expr='icontains')
    age = django_filters.CharFilter(lookup_expr='iexact')
    shape = django_filters.CharFilter(lookup_expr='iexact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    deletedAt = django_filters.CharFilter(lookup_expr='iexact')
    insurance = django_filters.CharFilter(field_name='insurance__id', lookup_expr='exact')
    type_patient = django_filters.CharFilter(field_name='type_patient__id', lookup_expr='exact')
    is_assured = django_filters.BooleanFilter(lookup_expr='exact')

    class Meta:
        model = Patient
        fields = {'id': ['exact']}


class Expenses_natureFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    type = django_filters.CharFilter(lookup_expr='exact')
    account_number = django_filters.CharFilter(lookup_expr='icontains')

    class Meta:
        model = Expenses_nature
        fields = {'id': ['exact']}


class Cash_movementFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    type = django_filters.CharFilter(lookup_expr='icontains')
    type_cash_movement = django_filters.CharFilter(lookup_expr='icontains')
    cash = django_filters.CharFilter(field_name='cash__id', lookup_expr='exact')
    expenses_nature = django_filters.CharFilter(field_name='expenses_nature__id', lookup_expr='exact')
    id = django_filters.CharFilter(field_name='cash__id', lookup_expr='exact')
    motive = django_filters.CharFilter(lookup_expr='icontains')
    amount_movement = django_filters.CharFilter(lookup_expr='icontains')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))

    class Meta:
        model = Cash_movement
        fields = {'id': ['exact']}


class Type_patientFilter(django_filters.FilterSet):
    is_default = django_filters.BooleanFilter(lookup_expr='exact')
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    title = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Type_patient
        fields = {'id': ['exact']}
        
class CategoryFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    code = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    billable = django_filters.CharFilter(lookup_expr='icontains')

    class Meta:
        model = Category
        fields = {'id': ['exact']}


class ExtendedGroupFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    name = django_filters.CharFilter(field_name='group__name', lookup_expr='exact')

    class Meta:
        model = ExtendedGroup
        fields = {'id': ['exact']}


class RegionFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    # hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    name = django_filters.CharFilter(lookup_expr='icontains')
    code = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Region
        fields = {'id': ['exact']}


class DetailsStock_movementFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    stock_movement = django_filters.CharFilter(field_name='stock_movement__id', lookup_expr='exact')
    storage_depots_dest = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')
    user = django_filters.CharFilter(field_name='user__id', lookup_expr='exact')
    total_amount = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.CharFilter(lookup_expr='iexact')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))
    date_range = django_filters.DateRangeFilter(field_name='createdAt')

    class Meta:
        model = DetailsStock_movement
        fields = {'id': ['exact']}


class CityFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    is_default = django_filters.BooleanFilter(lookup_expr='exact')
    # hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    name = django_filters.CharFilter(lookup_expr='icontains')
    code = django_filters.CharFilter(lookup_expr='icontains')
    region = django_filters.CharFilter(field_name='region__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = City
        fields = {'id': ['exact']}
class DistrictFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    is_default = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    city = django_filters.CharFilter(field_name='city__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = District
        fields = {'id': ['exact']}

class InsuranceFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    percent = django_filters.CharFilter(lookup_expr='icontains')
    number = django_filters.CharFilter(lookup_expr='icontains')
    # quantity = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Insurance
        fields = {'id': ['exact']}

class Storage_depotsFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    name = django_filters.CharFilter(lookup_expr='icontains')
    code = django_filters.CharFilter(lookup_expr='icontains')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    is_default = django_filters.BooleanFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Storage_depots
        fields = {'id': ['exact']}


class Stock_movementFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    movement_value = django_filters.CharFilter(lookup_expr='icontains')
    date_movement = django_filters.DateFilter(lookup_expr='exact')
    type_movement = django_filters.CharFilter(lookup_expr='icontains')
    reason_movement = django_filters.CharFilter(lookup_expr='icontains')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')
    storage_depots_dest = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')

    class Meta:
        model = Stock_movement
        fields = {'id': ['exact']}



class InventoryFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    date_inventory = django_filters.DateFilter(lookup_expr='exact')
    reason_inventory = django_filters.CharFilter(lookup_expr='icontains')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')

    class Meta:
        model = Inventory
        fields = {'id': ['exact']}


class SuppliesFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    total_amount = django_filters.CharFilter(lookup_expr='icontains')
    quantity = django_filters.CharFilter(lookup_expr='icontains')
    additional_info = django_filters.CharFilter(lookup_expr='icontains')
    reference_no = django_filters.CharFilter(lookup_expr='icontains')
    suppliers = django_filters.CharFilter(field_name='suppliers__id', lookup_expr='exact')
    arrival_price = django_filters.CharFilter(lookup_expr='icontains')
    supply_amount = django_filters.CharFilter(lookup_expr='icontains')
    arrival_date = django_filters.DateFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Supplies
        fields = {'id': ['exact']}

class StockFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    ingredient = django_filters.CharFilter(field_name='ingredient__id', lookup_expr='exact')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')    
    name = django_filters.CharFilter(field_name='ingredient__name', lookup_expr='icontains')
    quantity = django_filters.CharFilter(lookup_expr='icontains')
    quantity_two = django_filters.CharFilter(lookup_expr='icontains')
    quantity__lt = django_filters.NumberFilter(field_name='quantity', lookup_expr='lt')
    quantity__gt = django_filters.NumberFilter(field_name='quantity', lookup_expr='gt')
    date = django_filters.DateFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Stock
        fields = {'id': ['exact']}


class RecipesFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    total_amount = django_filters.CharFilter(lookup_expr='icontains')
    dish = django_filters.CharFilter(field_name='dish__id', lookup_expr='exact')
    dish_name = django_filters.CharFilter(method='filter_dish_name')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Recipes
        fields = {'id': ['exact']}
    
    def filter_dish_name(self, queryset, name, value):
        return queryset.filter(
            dish__name_language__icontains=value
        )




class BillsFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    district = django_filters.CharFilter(field_name='district__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    advance = django_filters.CharFilter(lookup_expr='icontains')
    phone_number = django_filters.CharFilter(lookup_expr='icontains')
    total_amount = django_filters.CharFilter(lookup_expr='icontains')
    bill_shape = django_filters.CharFilter(lookup_expr='icontains')
    additional_info = django_filters.CharFilter(lookup_expr='icontains')
    bill_type = django_filters.CharFilter(lookup_expr='icontains')
    patient = django_filters.CharFilter(field_name='patient__id', lookup_expr='exact')
    cash = django_filters.CharFilter(field_name='cash__id', lookup_expr='exact')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')
    is_proforma = django_filters.BooleanFilter(lookup_expr='exact')
    cashier_name = django_filters.CharFilter(lookup_expr='exact')
    net_payable = django_filters.CharFilter(lookup_expr='icontains')
    balance = django_filters.CharFilter(lookup_expr=('gt'))
    patient_type = django_filters.DateFilter(lookup_expr='exact')
    cash = django_filters.CharFilter(field_name='cash__id', lookup_expr='exact')
    cashier = django_filters.CharFilter(field_name='cash__user__id', lookup_expr='exact')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))
    date_range = django_filters.DateRangeFilter(field_name='createdAt')
    deleted = django_filters.BooleanFilter(lookup_expr='exact')

    class Meta:
        model = Bills
        fields = {'id': ['exact']}

class PatientSettlementFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    cashier = django_filters.CharFilter(field_name='cash__user_id', lookup_expr='exact')
    cash = django_filters.CharFilter(field_name='cash__id', lookup_expr='exact')
    cash_active = django_filters.CharFilter(field_name='cash__is_active', lookup_expr='exact')
    bills = django_filters.CharFilter(field_name='bills__id', lookup_expr='exact')
    amount_cash_gt = django_filters.NumberFilter(field_name='amount_cash', lookup_expr='gt')
    amount_om_gt = django_filters.CharFilter(field_name='amount_om', lookup_expr='gt')
    amount_momo_gt = django_filters.CharFilter(field_name='amount_momo', lookup_expr='gt')
    amount_bank_card_gt = django_filters.CharFilter(field_name='amount_bank_card', lookup_expr='gt')
    amount_prepaid_gt = django_filters.CharFilter(field_name='amount_prepaid', lookup_expr='gt')
    code = django_filters.CharFilter(lookup_expr='icontains')
    payment = django_filters.CharFilter(lookup_expr='icontains')
    wordings = django_filters.CharFilter(lookup_expr='icontains')
    patient = django_filters.CharFilter(field_name='patient__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    date = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = PatientSettlement
        fields = {'id': ['exact']}


class ArchiveFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    number_bills = django_filters.NumberFilter(lookup_expr='icontains')
    user = django_filters.CharFilter(field_name='user__id', lookup_expr='exact')
    month = django_filters.NumberFilter(lookup_expr='icontains')
    year = django_filters.NumberFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Archive
        fields = {'id': ['exact']}


class BackupFileFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    user = django_filters.CharFilter(field_name='user__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = BackupFile
        fields = {'id': ['exact']}


class SuppliersFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    phone = django_filters.CharFilter(lookup_expr='icontains')
    phone_representative = django_filters.CharFilter(lookup_expr='icontains')
    name_representative = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Suppliers
        fields = {'id': ['exact']}


class CashFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    wording = django_filters.CharFilter(lookup_expr='icontains')
    session = django_filters.CharFilter(lookup_expr='icontains')
    tyep_cash = django_filters.CharFilter(lookup_expr='icontains')
    open_date = django_filters.CharFilter(lookup_expr='iexact')
    close_date = django_filters.CharFilter(lookup_expr='iexact')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    is_transfer = django_filters.BooleanFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    cash_fund = django_filters.CharFilter(lookup_expr='icontains')
    user = django_filters.CharFilter(field_name='user__id', lookup_expr='exact')

    class Meta:
        model = Cash
        fields = {'id': ['exact']}


# class ModuleFilter(django_filters.FilterSet):
#     is_shared = django_filters.BooleanFilter(lookup_expr='exact')
#     hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
#     name = django_filters.CharFilter(lookup_expr='icontains')
#     is_active = django_filters.BooleanFilter(lookup_expr='exact')
#     createdAt = django_filters.DateFilter(lookup_expr='exact')

#     class Meta:
#         model = Module
#         fields = {'id': ['exact']}


class InventoryFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    date_inventory = django_filters.DateFilter(lookup_expr='exact')
    reason_inventory = django_filters.CharFilter(lookup_expr='icontains')
    storage_depots = django_filters.CharFilter(field_name='storage_depots__id', lookup_expr='exact')

    class Meta:
        model = Inventory
        fields = {'id': ['exact']}

class PatientAccountFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    balance = django_filters.CharFilter(lookup_expr='icontains')
    type_account = django_filters.CharFilter(lookup_expr='icontains')
    patient = django_filters.CharFilter(field_name='patient__id', lookup_expr='exact')

    class Meta:
        model = PatientAccount
        fields = {'id': ['exact']}


class DetailsPatientAccountFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    balance = django_filters.CharFilter(lookup_expr='icontains')
    balance_before = django_filters.CharFilter(lookup_expr='icontains')
    balance_after = django_filters.CharFilter(lookup_expr='icontains')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))
    patient_account = django_filters.CharFilter(field_name='patient_account__id', lookup_expr='exact')
    type_account = django_filters.CharFilter(field_name='patient_account__type_account', lookup_expr='exact')
    user = django_filters.CharFilter(field_name='user__id', lookup_expr='exact')

    class Meta:
        model = DetailsPatientAccount
        fields = {'id': ['exact'], 'type_operation':['exact']}


class DetailsBillsFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    quantity_ordered = django_filters.CharFilter(lookup_expr='iexact')
    amount_net = django_filters.CharFilter(lookup_expr='iexact')
    amount_gross = django_filters.CharFilter(lookup_expr='iexact')
    pun = django_filters.CharFilter(lookup_expr='iexact')
    pub = django_filters.CharFilter(lookup_expr='iexact')
    delivery = django_filters.CharFilter(lookup_expr='iexact')
    createdAt = django_filters.CharFilter(lookup_expr='iexact')
    updatedAt = django_filters.CharFilter(lookup_expr='iexact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    start_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('gte'), )
    end_date = django_filters.DateFilter(field_name='createdAt', lookup_expr=('lte'))
    date_range = django_filters.DateRangeFilter(field_name='createdAt')
    patient = django_filters.CharFilter(field_name='bills__patient__id', lookup_expr='exact')
    bills = django_filters.CharFilter(field_name='bills__id', lookup_expr='exact')
    patient_id = django_filters.CharFilter(field_name='patient__id', lookup_expr='exact')
    user = django_filters.CharFilter(field_name='user__id', lookup_expr='exact')

    class Meta:
        model = DetailsBills
        fields = {'id': ['exact']}

class DetailsInventoryFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    inventory = django_filters.CharFilter(field_name='inventory__id', lookup_expr='exact')
    amount = django_filters.CharFilter(lookup_expr='icontains')
    amount_adjusted = django_filters.CharFilter(lookup_expr='icontains')
    quantity_adjusted = django_filters.CharFilter(lookup_expr='icontains')

    class Meta:
        model = DetailsInventory
        fields = {'id': ['exact']}


class DeliveryInfoFilter(django_filters.FilterSet):
    bills = django_filters.CharFilter(field_name='bills__id', lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='bill__hospital__id', lookup_expr='exact')

    delivered = django_filters.BooleanFilter(lookup_expr='exact')
    delivery_service = django_filters.CharFilter(lookup_expr='icontains')
    address = django_filters.CharFilter(lookup_expr='icontains')
    delivery_man = django_filters.CharFilter(lookup_expr='icontains')

    expected_time = django_filters.DateTimeFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = DeliveryInfo
        fields = {
            'id': ['exact'],
        }

class CateringInfoFilter(django_filters.FilterSet):
    bills = django_filters.CharFilter(field_name='bills__id', lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='bill__hospital__id', lookup_expr='exact')

    company_name = django_filters.CharFilter(lookup_expr='icontains')
    contact_person = django_filters.CharFilter(lookup_expr='icontains')

    event_date = django_filters.DateFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = CateringInfo
        fields = {
            'id': ['exact'],
        }
class EventInfoFilter(django_filters.FilterSet):
    bills = django_filters.CharFilter(field_name='bills__id', lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='bill__hospital__id', lookup_expr='exact')

    event_name = django_filters.CharFilter(lookup_expr='icontains')
    organizer = django_filters.CharFilter(lookup_expr='icontains')
    location = django_filters.CharFilter(lookup_expr='icontains')

    event_start = django_filters.DateTimeFilter(lookup_expr='gte')
    event_end = django_filters.DateTimeFilter(lookup_expr='lte')

    paid = django_filters.BooleanFilter(lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = EventInfo
        fields = {
            'id': ['exact'],
        }