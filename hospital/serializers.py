from django.contrib.auth.models import Permission, Group, update_last_login
from django.contrib.contenttypes.models import ContentType
from rest_framework import serializers
from django.db.models import Sum
# from django.core.validators import RegexValidator
# from django.utils.translation import ugettext_lazy as _
from GH import settings
from hospital.models import Cash, Cash_movement, Category, CateringInfo, ComposeIngredient, ComposePreparation, DeliveryInfo, DetailsBillsIngredient, DetailsComposeIngredient, DetailsComposePreparation, DetailsPatientAccount, DetailsStock_movement, Dish, DishPreparation, EventInfo, Expenses_nature, ExtendedGroup, ExtendedPermission, Ingredient, MovementStock, Patient, PatientAccount, Promotion, PromotionAction, PromotionRule, RecipeIngredient, District, Insurance, Recipes, Stock, Stock_movement, Storage_depots, StructureArticle, User, Hospital,  \
    Suppliers, Supplies, DetailsSupplies, Bills, DetailsBills,PatientSettlement,\
    Inventory, DetailsInventory, City, Region, \
    Purchase_order, Archive, BackupFile, Type_patient, DeliveryInfo, EventInfo, CateringInfo, WarehouseTranslation

from GH.settings import SIMPLE_JWT as SIMPLE_JWT_SETTING
from track_actions.models import History
CUSTOM_TOKEN_HEADER = 'Custom-token'
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.tokens import RefreshToken


class TokenLogoutSerializer(serializers.Serializer):
    refresh = serializers.CharField()


class TokenRefreshSerializer(serializers.Serializer):
    refresh = serializers.CharField()

    def validate(self, attrs):
        refresh = RefreshToken(attrs['refresh'])

        data = {'access': str(refresh.access_token)}

        if SIMPLE_JWT_SETTING['ROTATE_REFRESH_TOKENS']:
            if SIMPLE_JWT_SETTING['BLACKLIST_AFTER_ROTATION']:
                try:
                    # Attempt to blacklist the given refresh token
                    refresh.blacklist()
                except AttributeError:
                    # If blacklist app not installed, `blacklist` method will
                    # not be present
                    pass

            refresh.set_jti()
            refresh.set_exp()

            data['refresh'] = str(refresh)

        return data


class MyTokenObtainPairSerializer(TokenObtainPairSerializer):
    custom_token_header = None

    def __init__(self, *args, **kwargs):
        super(MyTokenObtainPairSerializer, self).__init__(*args, **kwargs)
        request = self.context['request']
        self.custom_token_header = request.headers.get(CUSTOM_TOKEN_HEADER, None)

    @classmethod
    def get_token(cls, user):
        refresh_token = RefreshToken
        token = refresh_token.for_user(user)
        # Add custom claims
        # token['name'] = user.name
        # ...

        return token

    def validate(self, attrs):
        data = super().validate(attrs)
        refresh = self.get_token(self.user)

        data['refresh'] = str(refresh)
        data['access'] = str(refresh.access_token)

        if SIMPLE_JWT_SETTING['UPDATE_LAST_LOGIN']:
            update_last_login(None, self.user)

        return data



class DynamicFieldsModelSerializer(serializers.ModelSerializer):
    def __init__(self, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        super().__init__(*args, **kwargs)

        if fields is not None:
            allowed = set(fields)
            existing = set(self.fields)
            for field_name in existing - allowed:
                self.fields.pop(field_name)



class HospitalSerializer(DynamicFieldsModelSerializer):
    """
    Bifrost Hospital writable nested serializer
    """

    class Meta:
        model = Hospital
        fields = '__all__'

class ContentTypeSerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """

    class Meta:
        model = ContentType
        fields = '__all__'


# class ModuleSerializer(DynamicFieldsModelSerializer):
#     hospital = HospitalSerializer(many=False, fields=('id', 'name'))
#     contentType = ContentTypeSerializer(many=True)

#     class Meta:
#         model = Module
#         fields = '__all__'


class Type_patientSerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Type_diagnostic writable nested serializer
    """
    

    class Meta:
        model = Type_patient
        fields = '__all__'


class PermissionsSerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    model = serializers.CharField(source='content_type.model')

    class Meta:
        model = Permission
        fields = ('id', 'codename', 'name', 'model')


class ExtendedPermissionSerializer(DynamicFieldsModelSerializer):
    permission = PermissionsSerializer(read_only=True)
    hospital = HospitalSerializer(read_only=True)

    class Meta:
        model = ExtendedPermission
        fields = ['id', 'is_shared', 'permission', 'hospital']

class GroupSerializer(DynamicFieldsModelSerializer):
    permissions = PermissionsSerializer(many=True)

    class Meta:
        model = Group
        fields = ('id', 'name', 'permissions')


class ExtendedGroupSerializer(DynamicFieldsModelSerializer):
    # group = GroupSerializer(many=False)
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    group_name = serializers.SerializerMethodField()
    permissions = serializers.SerializerMethodField()

    class Meta:
        model = ExtendedGroup
        fields = ['id', 'group_name', 'hospital']

    def get_group_name(self, obj):
        return obj.group.name if obj.group else None

    def get_permissions(self, obj):
        return PermissionsSerializer(obj.get_permissions(), many=True).data
    
    # def get_hospital(self, obj):
    #     try:
    #         return HospitalSerializer(obj.extension.hospital).data if obj.extension.hospital else None
    #     except ExtendedGroup.DoesNotExist:
    #         return None
    

class UserMeSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost user writable nested serializer
    """
    
    groups = GroupSerializer(many=True)

    # patient = PatientSerializer(many=False)

    class Meta:
        model = User
        depth = 2
        exclude = (
            'password', 'first_name', 'last_name', 'email', 'is_staff', 'date_joined', 'user_permissions', 'last_login')


class RegionSerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost medical_areas writable nested serializer
    """

    class Meta:
        model = Region
        fields = '__all__'


class CitySerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost medical_areas writable nested serializer
    """
    region = RegionSerializer(many=False, required=False)

    class Meta:
        model = City
        fields = '__all__'

class DistrictSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost medical_areas writable nested serializer
    """
    city = CitySerializer(many=False, required=False)

    class Meta:
        model = District
        fields = '__all__'


class InsuranceSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    

    class Meta:
        model = Insurance
        fields = '__all__'


class PatientLightSerializer(DynamicFieldsModelSerializer):
    class Meta:
        model = Patient
        fields = ('id', 'code', 'name', 'phone')

class PatientSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    city = CitySerializer(many=False, required=False)
    insurance = InsuranceSerializer(many=False)
    type_patient = Type_patientSerializer(many=False, fields=('id', 'code', 'title'))
    account_patient = serializers.SerializerMethodField()
    # prepaid = serializers.SerializerMethodField()

    class Meta:
        model = Patient
        fields = '__all__'
        
    def get_account_patient(self, obj):
        return PatientAccountSerializer(obj.account_patient.filter(type_account='PREPAID'), many=True, fields=('id', 'balance')).data
    # def get_account_patient(self, obj):
    #     # Utilise le cache prefetch_related si disponible
    #     accounts = getattr(obj, 'all_accounts', None)
    #     if accounts is None:
    #         # fallback si pas de prefetch
    #         accounts = PatientAccount.objects.filter(patient_id=obj)
    #     return PatientAccountSerializer(accounts, many=True).data

    # def get_prepaid(self, obj):
    #     # Utilise le cache prefetch_related filtré si disponible
    #     prepaid_list = getattr(obj, 'prepaid_account', None)
    #     if prepaid_list is None:
    #         # fallback si pas de prefetch
    #         prepaid_list = PatientAccount.objects.filter(patient_id=obj, type_account='PREPAID')
        
    #     prepaid = prepaid_list[0] if prepaid_list else None
    #     return PatientAccountSerializer(prepaid, many=False).data if prepaid else None

class PatientAccountSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Patient Account account writable nested serializer
    """
    patient = PatientLightSerializer(many=False, read_only=True)
    # 

    class Meta:
        model = PatientAccount
        fields = '__all__'

class UserSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost user writable nested serializer
    """
    
    patient = PatientSerializer(many=False, fields=('id', 'name'))
    groups = GroupSerializer(many=True, fields=('id', 'name'))

    # patient = PatientSerializer(many=False)

    class Meta:
        model = User
        depth = 2
        exclude = (
            'password', 'first_name', 'last_name', 'email','is_staff', 'date_joined', 'user_permissions', 'last_login')



class DetailsPatientAccountSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost details patient account account writable nested serializer
    """
    patient_account = PatientAccountSerializer(many=False, required=False)
    user = UserSerializer(many=False, fields=('id', 'username'))
    

    class Meta:
        model = DetailsPatientAccount
        fields = '__all__'



class CategorySerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost medical_areas writable nested serializer
    """
    products_count = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()
    

    class Meta:
        model = Category
        fields = '__all__'
    
    def get_products_count(self, obj):
        return obj.products.count()
    
    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")



class Expenses_natureSerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    
    cash_movement_count = serializers.SerializerMethodField()

    class Meta:
        model = Expenses_nature
        fields = '__all__'

    def get_cash_movement_count(self, obj):
        return obj.expense_cash.all().count()


class CashSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    user = UserSerializer(many=False, fields=('id', 'username'))
    close_date = serializers.DateTimeField(format="%Y-%m-%d %H:%M:%S")
    

    class Meta:
        model = Cash
        fields = '__all__'


class SuppliersSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    

    class Meta:
        model = Suppliers
        fields = '__all__'


class Cash_movementSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Cash_movement writable nested serializer
    """
    cash = CashSerializer(many=False)
    cash_origin = CashSerializer(many=False)
    cash_destination = CashSerializer(many=False)
    expenses_nature = Expenses_natureSerializer(many=False)
    

    class Meta:
        model = Cash_movement
        fields = '__all__'



class ArchiveSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost archive writable nested serializer
    """
    user = UserSerializer(many=False, fields=('id', 'username'))
    

    class Meta:
        model = Archive
        fields = '__all__'


class BackupFileSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost BackupFile writable nested serializer
    """
    user = UserSerializer(many=False, fields=('id', 'username'))
    

    class Meta:
        model = BackupFile
        fields = '__all__'


class ChangePasswordSerializer(serializers.Serializer):
    """
    Serializer for password change endpoint.
    """
    old_password = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True)



class InventorySerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Inventory writable nested serializer
    """
    

    class Meta:
        model = Inventory
        fields = '__all__'


class Storage_depotsSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    
    name = serializers.SerializerMethodField()

    class Meta:
        model = Storage_depots
        fields = '__all__'


    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")

class SuppliesSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Supplies writable nested serializer
    """
    suppliers = SuppliersSerializer(many=False, fields=('id', 'name'))
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    

    class Meta:
        model = Supplies
        fields = '__all__'
        


class Stock_movementSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Stock_movement writable nested serializer
    """
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    

    class Meta:
        model = Stock_movement
        fields = '__all__'



class InventorySerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Inventory writable nested serializer
    """
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    

    class Meta:
        model = Inventory
        fields = '__all__'

class BillsSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Bills writable nested serializer
    """
    cash = CashSerializer(many=False, fields=('id', 'code', 'user'))
    district = DistrictSerializer(many=False, fields=('id', 'name'))
    patient = PatientSerializer(many=False, fields=('id', 'code', 'name', 'type_patient',))
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    
    details = serializers.SerializerMethodField()
    # sum_delivery = serializers.SerializerMethodField()
    amount_gross = serializers.SerializerMethodField()

    # patient_name= serializers.SerializerMethodField()
    # cash_code= serializers.SerializerMethodField()
    # cashier_name= serializers.SerializerMethodField()
# .aggregate(average_rating=Avg('rating'))['average_rating']
    class Meta:
        model = Bills
        exclude = ('updatedAt',)

    def get_details(self, obj):
        return DetailsBillsSerializer(obj.bills.all(), many=True).data
    
    # def get_sum_delivery(self, obj):
    #     return DetailsBills.objects.filter(bills_id=obj).all().aggregate(Sum('delivery'))['delivery__sum']    
    def get_amount_gross(self, obj):
        return DetailsBills.objects.filter(bills_id=obj).all().aggregate(Sum('amount_gross'))['amount_gross__sum']
    # def get_patient_name(self, obj):
    #     return obj.patient.name if obj.patient else None

    # def get_cashier(self, obj):
    #     return obj.cash.user.username if obj.cash else None
    # def get_cash_code(self, obj):
    #     return obj.cash.code if obj.cash else None


class BillsSerializerAnalysis(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Bills writable nested serializer
    """
    # patient = PatientSerializer(many=False, fields=('id', 'code', 'name', 'shape'))
    patient = PatientSerializer(many=False, fields=('id', 'name', 'type_patient'))
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    cash = CashSerializer(many=False, fields=('id', 'code','user'))
    
    # details = serializers.SerializerMethodField()
    # sum_delivery = serializers.SerializerMethodField()
    amount_gross = serializers.SerializerMethodField()

    # patient_name= serializers.SerializerMethodField()
    # cash_code= serializers.SerializerMethodField()
    # cashier_name= serializers.SerializerMethodField()

    class Meta:
        model = Bills
        exclude = ('updatedAt',)

    # def get_details(self, obj):
    #     return DetailsBillsSerializer(obj.bills.all(), many=True).data
    
    # def get_sum_delivery(self, obj):
    #     return DetailsBills.objects.filter(bills_id=obj).all().aggregate(Sum('delivery'))['delivery__sum']
    
    def get_amount_gross(self, obj):
        return DetailsBills.objects.filter(bills_id=obj).all().aggregate(Sum('amount_gross'))['amount_gross__sum']


class PatientSettlementSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost PatientSettlement writable nested serializer
    """
    cash = CashSerializer(many=False)
    patient = PatientSerializer(many=False, fields=('id', 'name', 'code'))
    bills = BillsSerializer(many=False, fields=('id', 'code','createdAt'))

    class Meta:
        model = PatientSettlement
        fields = '__all__'

from django.db.models import Prefetch

def get_stock_by_depot(self, obj):
    request = self.context.get('request')
    hospital = request.user.hospital
    language = getattr(request, "LANGUAGE_CODE", "fr")

    translation_prefetch = Prefetch(
        "storage_depots__translations",
        queryset=WarehouseTranslation.objects.filter(language=language),
        to_attr="filtered_translations"
    )

    stocks = (
        obj.stocks
        .filter(storage_depots__hospital=hospital)
        .select_related("storage_depots")
        .prefetch_related(translation_prefetch)
    )

    return StockByDepotSerializer(
        stocks,
        many=True,
        context=self.context
    ).data
class StockByDepotSerializer(DynamicFieldsModelSerializer):
    storage_depots_name = serializers.SerializerMethodField()
    class Meta:
        model = Stock
        fields = ["storage_depots_name", "quantity"]

    def get_storage_depots_name(self, obj):
        translations = getattr(obj.storage_depots, "filtered_translations", [])
        return translations[0].name if translations else None


class IngredientSerializer(DynamicFieldsModelSerializer):
    stock = serializers.SerializerMethodField()
    stock_by_depot = serializers.SerializerMethodField()
    stock_two = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()
    uid = serializers.SerializerMethodField()
    class Meta:
        model = Ingredient
        fields = "__all__"
    def get_uid(self, obj):
        return f"ingredient-{obj.id}"
    
    def get_stock_by_depot(self, obj):
        request = self.context.get('request', None)
        hospital = request.user.hospital
        stocks = obj.stocks.filter(storage_depots__hospital=hospital).select_related("storage_depots")
        return StockByDepotSerializer(stocks, many=True).data

    def get_stock(self, obj):
        request = self.context.get('request', None)
        hospital = request.user.hospital
        total = obj.stocks.filter(storage_depots__hospital=hospital).aggregate(total=Sum("quantity"))["total"]
        return total or 0
        
    def get_stock_two(self, obj):
        request = self.context.get('request', None)
        hospital = request.user.hospital
        total = obj.stocks.filter(storage_depots__hospital=hospital).aggregate(total=Sum("quantity_two"))["total"]
        return total or 0
    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")
    

class DetailsStock_movementSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost DetailsStock_movement writable nested serializer
    """
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    storage_depots_dest = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    stock_movement = Stock_movementSerializer(many=False)
    ingredient = IngredientSerializer(many=False)
    

    class Meta:
        model = DetailsStock_movement
        fields = '__all__'

class StockSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Supplies writable nested serializer
    """
    ingredient = IngredientSerializer(many=False, fields=('id', 'code', 'name'))
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    

    class Meta:
        model = Stock
        fields = '__all__'
        
class DetailsInventorySerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost DetailsStock_movement writable nested serializer
    """
    user = UserSerializer(many=False)
    storage_depots = Storage_depotsSerializer(many=False, fields=('id', 'name'))
    inventory = InventorySerializer(many=False, fields=('id', 'name'))
    ingredient = IngredientSerializer(many=False)
    

    class Meta:
        model = DetailsInventory
        fields = '__all__'

class DishSerializer(DynamicFieldsModelSerializer):
    # recipe_ingredients = RecipeIngredientSerializer(many=True, read_only=True)
    cost = serializers.FloatField(read_only=True)
    # category = CategorySerializer()
    category = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()
    price = serializers.SerializerMethodField()

    class Meta:
        model = Dish
        fields = ["id", "code","name","name_language", "price", "category", "is_delivery","is_active", "preparation_time", "cost", "createdAt"]

    def get_category(self, obj):
        request = self.context.get('request')
        return CategorySerializer(
            obj.category,
            context={'request': request}
        ).data
    
    def get_price(self, obj):
        request = self.context.get('request', None)
        if request:
            hospital = request.user.hospital
            try:
                sp = obj.prices.get(hospital=hospital, is_active=True)
                return sp.price
            except StructureArticle.DoesNotExist:
                return None
        else:
            try:
                sp = obj.prices.get(is_active=True)
                return sp.price
            except StructureArticle.DoesNotExist:
                return None
        

    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")
class ComposeIngredientSerializer(DynamicFieldsModelSerializer):
    # recipe_ingredients = RecipeIngredientSerializer(many=True, read_only=True)
    # category = CategorySerializer()
    uid = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()

    class Meta:
        model = ComposeIngredient
        fields = '__all__'
    def get_uid(self, obj):
        return f"compose-{obj.id}"
    
    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")
    
class ComposePreparationSerializer(DynamicFieldsModelSerializer):
    # recipe_ingredients = RecipeIngredientSerializer(many=True, read_only=True)
    # category = CategorySerializer()
    uid = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()

    class Meta:
        model = ComposePreparation
        fields = '__all__'
    def get_uid(self, obj):
        return f"compose-{obj.id}"
    
    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")
    
class PromotionSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    name = serializers.SerializerMethodField()
     
    class Meta:
        model = Promotion
        fields = '__all__'

    def get_name(self, obj):
        request = self.context.get('request', None)
        lang = getattr(request, 'LANGUAGE_CODE', 'fr')

        # Essaie la langue de la requête
        translations = {
            t.language: t.name
            for t in obj.translations.all()
        }

        return translations.get(lang) or translations.get("fr")

class DetailsBillsSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    bills = BillsSerializer(many=False, fields=('id', 'code', 'patient', 'doctor'))
    user = UserSerializer(many=False, fields=('id', 'username'))
    promotion = PromotionSerializer(many=False, fields=('id', 'name'))
    # dish = DishSerializer(many=False)
    dish = serializers.SerializerMethodField()
    storage_depots = serializers.SerializerMethodField()
    options = serializers.SerializerMethodField()
    

    class Meta:
        model = DetailsBills
        exclude = ('updatedAt',)
    
    def get_dish(self, obj):
        request = self.context.get('request')
        return DishSerializer(
            obj.dish,
            context=self.context
        ).data
    def get_storage_depots(self, obj):
        request = self.context.get('request')
        return Storage_depotsSerializer(
            obj.storage_depots,
            context=self.context
        ).data

    def get_options(self, obj):
        return DetailsBillsIngredientSerializer(obj.options.all(), many=True).data

class DeliveryInfoSerializer(DynamicFieldsModelSerializer):
    bills = BillsSerializer(many=False, fields=('id', 'code', 'patient', 'doctor'))  # ou BillsMiniSerializer si tu as
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
     
    class Meta:
        model = DeliveryInfo
        fields = '__all__'
class DetailsBillsIngredientSerializer(DynamicFieldsModelSerializer):
    details_bills = DetailsBillsSerializer(many=False, fields=('id',)) 
    ingredient = IngredientSerializer(many=False, fields=('id', 'name', 'price_per_unit'))
    compose_ingredient = ComposeIngredientSerializer()
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
     
    class Meta:
        model = DetailsBillsIngredient
        fields = '__all__'
class MovementStockSerializer(DynamicFieldsModelSerializer):
    ingredient = IngredientSerializer(many=False, fields=('id', 'name'))
    compose_ingredient = ComposeIngredientSerializer()
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
     
    class Meta:
        model = MovementStock
        fields = '__all__'


class PromotionRuleSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    type_patient = Type_patientSerializer(many=False, fields=('id', 'title'))
    category = CategorySerializer(many=False, fields=('id', 'name'))
    dish = DishSerializer(many=False, fields=('id', 'name'))
    promotion = PromotionSerializer(many=False, fields=('id', 'name'))
     
    class Meta:
        model = PromotionRule
        fields = '__all__'

class PromotionActionSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    promotion = PromotionSerializer(many=False, fields=('id', 'name'))
     
    class Meta:
        model = PromotionAction
        fields = '__all__'
class CateringInfoSerializer(DynamicFieldsModelSerializer):
    bills = BillsSerializer(many=False, fields=('id', 'code', 'patient', 'doctor'))
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = CateringInfo
        fields = '__all__'

class EventInfoSerializer(DynamicFieldsModelSerializer):
    bills = BillsSerializer(many=False, fields=('id', 'code', 'patient', 'doctor'))
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = EventInfo
        fields = '__all__'
class RecipesSerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost Supplies writable nested serializer
    """
    dish = DishSerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = Recipes
        fields = '__all__'
        
class RecipeIngredientSerializer(DynamicFieldsModelSerializer):
    recipes = RecipesSerializer(many=False, fields=('id', 'total_amount', 'user'))
    dish = DishSerializer(many=False, fields=('id', 'name', 'cost'))
    # ingredient = IngredientSerializer()
    compose_ingredient = ComposeIngredientSerializer(many=False, fields=('id', 'name'))
    # compose_preparation = ComposePreparationSerializer(many=False, fields=('id', 'name'))
    ingredient = IngredientSerializer(many=False, fields=('id', 'name'))
    cost = serializers.FloatField(read_only=True)

    class Meta:
        model = RecipeIngredient
        fields = ["id", "ingredient","compose_ingredient", "recipes", "quantity", "cost", "dish", "createdAt"]

    # def get_ingredient(self, obj):
    #     request = self.context.get('request')
    #     return IngredientSerializer(
    #         obj.ingredient,
    #         context={'request': request}
    #     ).data
class DetailsComposeIngredientSerializer(DynamicFieldsModelSerializer):
    compose_ingredient = ComposeIngredientSerializer()
    # ingredient = IngredientSerializer()
    ingredient = serializers.SerializerMethodField()
    cost = serializers.FloatField(read_only=True)

    class Meta:
        model = DetailsComposeIngredient
        fields = '__all__'

    def get_ingredient(self, obj):
        request = self.context.get('request')
        return IngredientSerializer(
            obj.ingredient,
            context={'request': request}
        ).data    #     ).data
class StructureArticleSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    # ingredient = IngredientSerializer()
    dish = DishSerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = StructureArticle
        fields = '__all__'

class DetailsComposePreparationSerializer(DynamicFieldsModelSerializer):
    compose_preparation = ComposePreparationSerializer()
    compose_ingredient = ComposeIngredientSerializer()
    ingredient = IngredientSerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = DetailsComposePreparation
        fields = '__all__'

class DetailsSuppliesSerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost         model = DetailsSupplies
 writable nested serializer
    """
    supplies = SuppliesSerializer(many=False, fields=('id', 'code', 'createdAt', 'suppliers'))
    ingredient = IngredientSerializer(many=False, fields=('id', 'name'))
    

    class Meta:
        model = DetailsSupplies
        fields = '__all__'

class DetailsInventorySerializer(DynamicFieldsModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost DetailsStock_movement writable nested serializer
    """
    user = UserSerializer(many=False)
    
    ingredient = IngredientSerializer(many=False, fields=('id', 'name'))
    inventory = InventorySerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = DetailsInventory
        fields = '__all__'

class HistorySerializer(DynamicFieldsModelSerializer):
    # hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    """
    Bifrost patient writable nested serializer
    """
    user = UserSerializer(many=False, fields=('id', 'username'))

    class Meta:
        model = History
        fields = ('id', 'user', 'action', 'created_at', 'table_name')


class DishPreparationSerializer(DynamicFieldsModelSerializer):
    dish_name = serializers.CharField(source="dish.name", read_only=True)
    dish = DishSerializer(many=False, fields=('id', 'name'))

    class Meta:
        model = DishPreparation
        fields = (
            "id",
            "dish",
            "dish_name",
            "deleted",
            "quantity",
            "createdAt",
        )
        read_only_fields = ("id", "createdAt", "dish_name")