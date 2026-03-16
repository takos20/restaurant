import subprocess

from django.conf import settings
import os
from django.core.management.base import BaseCommand
from django.db import connections
from django.utils import timezone
import psycopg2
import logging
from hospital.models import BILL_SHAPE_CHOICES, BILLS_CHOICES, PATIENT_SHAPE_CHOICES, Bills, Archive, Cash, CateringInfo, ComposeIngredient, ComposePreparation, ComposePreparationTranslation, DeliveryInfo, DetailsComposeIngredient, DetailsComposePreparation, DetailsPatientAccount, EventInfo, ExtendedPermission, Ingredient, MovementStock, Patient, BackupFile, PromotionRule, Stock, Storage_depots
from datetime import datetime, timedelta, date
from django.core.management import call_command
from django.db import models
from django.db.models import Sum
import re
from decimal import Decimal
import logging
from rest_framework.exceptions import ValidationError
logger = logging.getLogger(__name__)

from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType

from rest_framework.permissions import BasePermission
from django.conf import settings

class SyncPermission(BasePermission):

    def has_permission(self, request, view):
        return request.headers.get("X-SYNC-KEY") == settings.SYNC_SECRET_KEY
# @transaction.atomic
# def get_or_fix_stock(hospital, depot, ingredient=None, compose_ingredient=None):

#     queryset = Stock.objects.filter(
#         hospital=hospital,
#         storage_depots=depot,
#         deleted=False
#     )

#     if ingredient:
#         queryset = queryset.filter(ingredient=ingredient)
#     else:
#         queryset = queryset.filter(compose_ingredient=compose_ingredient)

#     stocks = list(queryset.order_by('-id'))

#     if not stocks:
#         return None

#     if len(stocks) == 1:
#         return stocks[0]

#     # 🔥 Fusion automatique des doublons
#     main_stock = stocks[0]
#     total_quantity = sum(s.quantity for s in stocks)

#     main_stock.quantity = total_quantity
#     main_stock.save()

#     # Supprimer les doublons
#     for duplicate in stocks[1:]:
#         duplicate.delete()

#     return main_stock

# stock = get_or_fix_stock(
#     hospital=user.hospital,
#     depot=storage_depot,
#     ingredient=opt.ingredient
# )

# if stock:
#     stock.quantity -= qty_out
#     stock.save()
def setup_hospital_permissions(hospital):
    """
    Configure les permissions de base pour un hôpital
    """
    # Modèles pour lesquels créer des permissions
    models_to_grant = [
        "history",
"user",
"bills",
"cash",
"category",
"city",
"detailsbills",
"dish",
"hospital",
"ingredient",
"insurance",
"patient",
"promotion",
"suppliers",
"type_patient",
"supplies",
"stock_movement",
"region",
"recipeingredient",
"purchase_order",
"promotionrule",
"promotionaction",
"patientsettlement",
"patientaccount",
"movementstock",
"inventory",
"extendedpermission",
"extendedgroup",
"expenses_nature",
"eventinfo",
"district",
"dishpreparation",
"detailssupplies",
"detailspatientaccount",
"detailsinventory",
"detailsbillsingredient",
"deliveryinfo",
"cateringinfo",
"cash_movement",
"backupfile",
"archive",
"promotiontranslation",
"ingredienttranslation",
"dishtranslation",
"categorytranslation",
"others",
"recipes",
"composeingredienttranslation",
"detailscomposeingredient",
"composeingredient",
"composepreparation",
"detailscomposepreparation",
"composepreparationtranslation",
"storage_depots",
"detailsstock_movement",
"warehousetranslation",
"stock"
    ]
    
    permissions_created = []
    
    for model_name in models_to_grant:
        try:
            content_type = ContentType.objects.get(model=model_name)
            
            # Permissions CRUD
            for action in ['view', 'add', 'change', 'delete']:
                permission = Permission.objects.get(
                    content_type=content_type,
                    codename=f'{action}_{model_name}'
                )
                
                # Créer la liaison ExtendedPermission
                hospital_perm, created = ExtendedPermission.objects.get_or_create(
                    hospital=hospital,
                    permission=permission,
                    defaults={'is_active': True}
                )
                
                if created:
                    permissions_created.append(permission.codename)
        
        except ContentType.DoesNotExist:
            print(f"ContentType not found for model: {model_name}")
        except Permission.DoesNotExist:
            print(f"Permission not found for model: {model_name}")
    
    return permissions_created
from django.utils import timezone

def save_bills(get_bills, request):
    if request.data['bill_type'] == 'DELIVERY':
        get_bills.address = request.data.get('address', None)
        get_bills.delivery_fee = request.data.get('delivery_fee', None)
        get_bills.delivery_service = request.data.get('delivery_service', None)
        get_bills.delivery_man = request.data.get('delivery_man', None)
        DeliveryInfo.objects.create(hospital = request.user.hospital, bills_id = get_bills.id, address = request.data.get('address', None),delivery_fee = request.data.get('delivery_fee', None),delivery_service = request.data.get('delivery_service', None),delivery_man = request.data.get('delivery_man', None))

    if request.data['bill_type'] == 'EVENT':
        get_bills.event_start = request.data.get('event_start', None)
        get_bills.event_end = request.data.get('event_end', None)
        get_bills.event_name = request.data.get('event_name', None)
        get_bills.organizer = request.data.get('organizer', None)
        get_bills.location = request.data.get('location', None)
        get_bills.contract_amount = request.data.get('contract_amount', None)
        get_bills.estimated_guests = request.data.get('estimated_guests', None)

        EventInfo.objects.create(hospital = request.user.hospital, bills_id = get_bills.id, event_start = request.data.get('event_start', None),event_end = request.data.get('event_end', None),event_name = request.data.get('event_name', None),organizer = request.data.get('organizer', None),location = request.data.get('location', None),contract_amount = request.data.get('contract_amount', None),estimated_guests = request.data.get('estimated_guests', None))

    if request.data['bill_type'] == 'CATERING':
        get_bills.contact_person = request.data.get('contact_person', None)
        get_bills.advance_payment = request.data.get('advance_payment', None)
        get_bills.company_name = request.data.get('company_name', None)
        get_bills.balance_due = request.data.get('balance_due', None)
        get_bills.event_date = request.data.get('event_date', None)
        get_bills.event_location = request.data.get('event_location', None)

        CateringInfo.objects.create(hospital = request.user.hospital, bills_id = get_bills.id, contact_person = request.data.get('contact_person', None),advance_payment = request.data.get('advance_payment', None),company_name = request.data.get('company_name', None),balance_due = request.data.get('balance_due', None),event_date = request.data.get('event_date', None),event_location = request.data.get('event_location', None))

    get_bills.save()

def rule_match(rule, context):

    bill = context["bill"]
    client = context["client"]
    # total_amount = context["total_amount"]
    current_time = context["current_time"]
    # history_count = context["history_count"]
    # payment_method = context.get("payment_method")

    # 1️⃣ Vérifier période de validité
    if rule.promotion.start_date and current_time < rule.promotion.start_date:
        return False

    if rule.promotion.end_date and current_time > rule.promotion.end_date:
        return False

    # 2️⃣ Vérifier type client
    if rule.type_patient:
        if not client or client.type_patient != rule.type_patient:
            return False

    # 3️⃣ Vérifier mode de paiement
    # if rule.payment_method:
    #     if payment_method != rule.payment_method:
    #         return False

    # 4️⃣ Vérifier heure (Happy Hour)
    if rule.start_hour and rule.end_hour:
        now_time = current_time.time()
        if not (rule.start_hour <= now_time <= rule.end_hour):
            return False

    # 5️⃣ Filtrer items concernés
    filtered_items = bill
    if rule.dish:
        filtered_items = filtered_items.filter(dish=rule.dish)

    if rule.category:
        filtered_items = filtered_items.filter(dish__catagory=rule.category)

    # quantity = sum(item.quantity for item in filtered_items)

    # 6️⃣ Vérifier quantité minimale
    # if rule.min_quantity:
    #     if quantity < rule.min_quantity:
    #         return False

    # 7️⃣ Vérifier montant minimal
    # if rule.min_amount:
    #     if total_amount < rule.min_amount:
    #         return False

    # 8️⃣ Vérifier modulo (fidélité / N-ième)
    # if rule.modulo:
    #     if history_count % rule.modulo != rule.modulo_equals:
    #         return False

    return True
def apply_action(promotion, bill):
    action = promotion.promotionaction_set.first()

    if action.action_type == 'PERCENT':
        item_discount = (bill.amount_net * action.value) / 100
        bill.amount_net -= item_discount
        bill.delivery = action.value
        bill.promotion_id = promotion.id
        bill.save()

    elif action.action_type == 'FREE':
        # make_last_dish_free()
        pass

    elif action.action_type == 'FIXED':
        bill.amount_net -= action.value
        bill.delivery = str(action.value) + 'F'
        bill.promotion_id = promotion.id
        bill.save()
def apply_promotions(bill):
    rules = PromotionRule.objects.filter(promotion__is_active=True)\
                                 .order_by('-promotion__priority')
    context = {
                    "bill": bill,
                    "client": bill.patient,
                    "current_time": timezone.now(),
                }
    result = False
    print(context)
    cummulative = False
    for rule in rules:
        if rule_match(rule, context):
            result=True
            cummulative = rule.promotion.cumulative
            apply_action(rule.promotion, bill)
        
    return result, cummulative

def apply_percent_discount(bill, percent, promotion_name, filtered_items=None):
    """
    Applique une réduction en pourcentage.
    percent = 10 pour 10%
    filtered_items = queryset des items concernés (optionnel)
    """

    if not filtered_items:
        filtered_items = bill.items.all()

    total_discount = 0

    for item in filtered_items:
        item_discount = (item.total_price * percent) / 100

        item.discount_amount += item_discount
        item.promotion_name = promotion_name
        item.save()

        total_discount += item_discount

    bill.discount_total += total_discount
    bill.recalculate_total()

    return total_discount

def apply_fixed_discount(bill, amount, promotion_name, filtered_items=None):

    if not filtered_items:
        filtered_items = bill.items.all()

    subtotal = sum(item.total_price for item in filtered_items)

    if subtotal == 0:
        return 0

    if amount > subtotal:
        amount = subtotal

    total_discount = 0

    for item in filtered_items:
        ratio = item.total_price / subtotal
        item_discount = amount * ratio

        item.discount_amount += item_discount
        item.promotion_name = promotion_name
        item.save()

        total_discount += item_discount

    bill.discount_total += total_discount
    bill.recalculate_total()

    return total_discount
def get_prepaid_account_detail(request, user, source, reference_id):
    """
    Récupère le détail du compte prépayé pour une facture spécifique
    
    Args:
        request: Request object contenant patient et bills
        user: User object avec l'hôpital
        
    Returns:
        DetailsPatientAccount: Détail du compte ou None
        
    Raises:
        ValidationError: Si les données sont invalides
    """
    # Validation
    patient_id = request.data.get('patient')
    reference_id = reference_id
    
    if not patient_id:
        raise ValidationError({'patient': 'Patient ID is required'})
    if not reference_id:
        raise ValidationError({'reference_id': 'Reference ID is required'})
    
    try:
        # Requête optimisée
        get_detail_account = DetailsPatientAccount.objects.select_related(
            'patient_account',
            'patient_account__patient',
            'hospital'
        ).filter(
            type_operation='CREDIT',
            hospital=user.hospital,
            patient_account__patient_id=patient_id,
            patient_account__type_account='PREPAID',
            source=source,
            reference_id=reference_id
        ).order_by('-createdAt').first()  # Explicitement le plus récent
        
        if not get_detail_account:
            logger.warning(
                f"No prepaid transaction found for patient {patient_id}, "
                f"bill {reference_id} in hospital {user.hospital.id}"
            )
            return None
            
        return get_detail_account
        
    except Exception as e:
        logger.error(f"Error fetching prepaid account detail: {e}", exc_info=True)
        raise ValidationError({'error': 'Unable to fetch account details'})


def get_fk_stats_by_model(model, start=None,end=None, exclude_fields=None,rename_map=None,filters=None,elt_to_sum=None):
    """
        Calcule le chiffre d'affaires total par ForeignKey d'un modèle donné,
        avec possibilité de renommer certaines clés dans le résultat.

        Args:
        model: le modèle Django (ex: DetailsBills)
        start: date de début (optionnel)
        end: date de fin (optionnel)
        exclude_fields: liste des noms de ForeignKey à exclure
        rename_map: dictionnaire {NomFKOriginal: NouveauNom} pour renommer les clés

        Returns:
        dict: {NomDuModèleFK: total_amount_net}
    """
    
    if start and end:
        filters["createdAt__range"] = [start, end]

    results = {}
    exclude_fields = exclude_fields or []
    rename_map = rename_map or {}

    for field in model._meta.get_fields():
        if isinstance(field, ForeignKey) and field.name not in exclude_fields:
            fk_model_name = field.related_model.__name__
            # Appliquer le renommage si défini
            fk_model_name = rename_map.get(fk_model_name, fk_model_name)

            # Calcul du total pour cette ForeignKey
            total = (
            model.objects
            .filter(**filters)
            .exclude(**{f"{field.name}__isnull": True})
            .aggregate(total=Sum(elt_to_sum))["total"]
            ) or 0
            if total != 0:
                results[fk_model_name] = total

    return results
 

# import subprocess
from django.db.models import F



def get_days_since_last_update(last_save_time):
    time_diff=timezone.now()-last_save_time
    return time_diff.days

def destocker(user, ingredient, quantity, facture_id, storage_depots, hospital, source):
    if storage_depots is None:
        get_storage_depots = Storage_depots.objects.filter(is_default=True).last()
        storage_depots = get_storage_depots.id
    stock, created = Stock.objects.get_or_create(
        hospital=user.hospital,
        ingredient=ingredient,
        storage_depots_id=storage_depots,
        defaults={"quantity": 0}
    )

    stock.quantity -= quantity
    stock.save()
    MovementStock.objects.create(
        ingredient=ingredient,
        hospital = hospital,
        type="EXIT",
        quantity=quantity,
        source=source,
        reference_id=facture_id
    )
def destocker_compose(user, compose_ingredient, quantity, facture_id, storage_depots,hospital, source):
    if storage_depots is None:
        get_storage_depots = Storage_depots.objects.filter(is_default=True).last()
        storage_depots = get_storage_depots.id
    stock, created = Stock.objects.get_or_create(
        hospital=user.hospital,
        compose_ingredient_id=compose_ingredient.id,
        storage_depots_id=storage_depots,
        defaults={"quantity": 0}
    )

    stock.quantity -= quantity
    stock.save()

    get_obj = ComposePreparation.objects.filter(compose_ingredient_id = compose_ingredient.id).last()
    if get_obj:
        details = DetailsComposeIngredient.objects.filter(compose_ingredient_id = compose_ingredient.id).all()
        for detail in details:
            # qty = (request.data['stock_quantity'] * detail.quantity) / detail.compose_ingredient.stock_quantity

            stock_quantity = Decimal(quantity)

            qty = (
                stock_quantity
                * Decimal(detail.quantity)
                / Decimal(detail.compose_ingredient.stock_quantity)
            )
            if detail.ingredient.price_per_unit:

                cost = qty * detail.ingredient.price_per_unit
            else:
                cost = 0

            get_detail_preparation = DetailsComposePreparation.objects.filter(hospital=user.hospital,compose_preparation_id=get_obj.id, compose_ingredient_id=compose_ingredient.id,ingredient=detail.ingredient).last()
            if get_detail_preparation:
                get_detail_preparation.quantity += qty
                get_detail_preparation.cost += cost
                get_detail_preparation.save()
            else:
                DetailsComposePreparation.objects.create(hospital=user.hospital,compose_preparation_id=get_obj.id, ingredient=detail.ingredient,compose_ingredient_id=compose_ingredient.id, quantity=qty, user=user, cost=cost)

        get_obj.stock_quantity -= float(quantity)
        get_obj.save()
    else:
        compose = ComposePreparation.objects.create(
                hospital=user.hospital,
                total_amount=0,
                stock_quantity=0,
                compose_ingredient_id=compose_ingredient.id,
                user_id = user.id
            )
        compose.stock_quantity -= float(quantity)
        compose.save()
        for translate in compose.compose_ingredient.name_language:
            get_translate = ComposePreparationTranslation.objects.filter(hospital = user.hospital, compose_preparation_id=compose.id, language=translate['language']).last()
            if get_translate:
                get_translate.name = translate['name']
                get_translate.save()
            else:
                name = translate['name']
                ComposePreparationTranslation.objects.create(hospital = user.hospital, user=user, compose_preparation_id=compose.id, language=translate['language'], name = name)

        
        details = DetailsComposeIngredient.objects.filter(compose_ingredient_id = compose_ingredient.id).all()
        for detail in details:
            # qty = (request.data['stock_quantity'] * detail.quantity) / detail.compose_ingredient.stock_quantity

            stock_quantity = Decimal(quantity)

            qty = (
                stock_quantity
                * Decimal(detail.quantity)
                / Decimal(detail.compose_ingredient.stock_quantity)
            )
            if detail.ingredient.price_per_unit:

                cost = qty * detail.ingredient.price_per_unit
            else:
                cost = 0

            get_detail_preparation = DetailsComposePreparation.objects.filter(hospital=user.hospital,compose_preparation_id=compose.id,compose_ingredient_id=compose_ingredient.id, ingredient=detail.ingredient).last()
            if get_detail_preparation:
                get_detail_preparation.quantity = qty
                get_detail_preparation.cost = cost
                get_detail_preparation.save()
            else:
                DetailsComposePreparation.objects.create(hospital=user.hospital,compose_preparation_id=compose.id, ingredient=detail.ingredient,compose_ingredient_id=compose_ingredient.id, quantity=qty, user=user, cost=cost)

    MovementStock.objects.create(
        compose_ingredient=compose_ingredient,
        hospital = hospital,
        type="EXIT",
        quantity=quantity,
        source=source,
        reference_id=facture_id
    )
from decimal import Decimal

def price_sell_ligne(ligne):
    added = ligne.options.filter(action="ADD").aggregate(
        total=Sum("impact_price")
    )["total"] or 0

    price_final = ligne.pun + added
    return price_final * ligne.quantity_served
from django.db.models import FloatField
from django.db.models.functions import Coalesce

def split_entry_exit(queryset):
    data = {row['types']: row['total'] for row in queryset}
    return (
        data.get('EXIT', 0) or 0,
        data.get('ENTRY', 0) or 0
    )
def checkContent(content):
    if type(content) == str:
        cont=content
    elif math.isnan(content):
        cont = 'Non specifie'
    else:
        cont=content
    
    return cont


def checkNumber(content):
    if type(content) == int:
        cont=content
    elif math.isnan(content):
        cont = 0
    else:
        cont=content
    
    return cont
def checkBool(content):
    if content == 'oui':
        cont = True
    elif content == 'non':
        cont = False
    else:
        cont = True
    return cont

import math
def checkContentPhone(content):
    if type(content) == str:
        cont=content
    elif str(content) == 'nan':
        cont='699999999'
    else:
        cont=content
    return cont


def split_rules(rules):
    fidelity = []
    promos = []

    for r in rules:
        if r.get("type_advantage") == "PROMO":
            promos.append(r)
        else:
            fidelity.append(r)

    return fidelity, promos

def check_active_promo(promos):
    today = date.today()
    for p in promos:
        start = datetime.strptime(p["start_date"], "%Y-%m-%d").date()
        end = datetime.strptime(p["end_date"], "%Y-%m-%d").date()

        if start <= today <= end:
            return {
                "matched": True,
                "reduction": float(p["reduction"]),
                "type": "PROMO"
            }

    return {"matched": False}

def check_fidelity(count_dish: int, rules: list):
    for rule in rules:
        print(count_dish, count_dish % int(rule["number_dishes"]))
        if count_dish > 1 and count_dish % int(rule["number_dishes"]) == 1:
            reduction = float(rule["reduction"])
            return {
                "matched": True,
                "reduction": reduction,
                "type": "FREE" if reduction == 100 else "HALF",
                "rule_number": int(rule["number_dishes"])
            }

    return {"matched": False}

#Biggest reduction must be checked first (10 before 5).
def normalize_rules(rules):
    normalized = []

    for r in rules:
        if 'start_date' in r:
            normalized.append({
            "number_dishes": int(r["number_dishes"]),
            "reduction": float(r["reduction_percentage"]),
            "type_advantage": r["type_advantage"],
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "type": "FREE" if float(r["reduction_percentage"]) == 100 else "HALF"
        })
            
        else:
            normalized.append({
                "number_dishes": int(r["number_dishes"]),
                "reduction": float(r["reduction_percentage"]),
                "type_advantage": r["type_advantage"],
                "type": "FREE" if float(r["reduction_percentage"]) == 100 else "HALF"
            })

    # sort DESC so 10 comes before 5
    return sorted(normalized, key=lambda x: x["number_dishes"], reverse=True)

# def check_remise(count_dish: int, rules: list):
#     """
#     Returns the best matching rule for this dish count
#     """
#     for rule in rules:
#         if count_dish > 1 and count_dish % rule["number_dishes"] == 1:
#             return {
#                 "matched": True,
#                 "reduction": rule["reduction"],
#                 "type": rule["type"]
#             }

#     return {"matched": False, "reduction": 0}

def check_fidelity_only(count_dish: int, rules: list):
    fidelity_rules = [r for r in rules if r.get("type_advantage") == "FIDELITY"]
    for rule in fidelity_rules:
        if count_dish > 1 and count_dish % int(rule["number_dishes"]) == 1:
            reduction = float(rule["reduction"])
            return {
                "matched": True,
                "reduction": reduction,
                "type": "FREE" if reduction == 100 else "HALF",
                "rule_number": int(rule["number_dishes"])
            }

    return {"matched": False}

def get_future_remise_notification(count_dish: int, rules: list):
    next_count = count_dish + 1
    result = check_fidelity_only(next_count, rules)

    return {
        "should_notify": result["matched"],
        **result
    }

def get_applicable_reduction(count_dish: int, all_rules: list):
    fidelity_rules, promo_rules = split_rules(all_rules)
    print(fidelity_rules, promo_rules)
    # 1️⃣ Check promo first
    promo = check_active_promo(promo_rules)
    
    if promo["matched"]:
        return {
            "should_apply": True,
            **promo
        }

    # 2️⃣ Else check fidelity
    fidelity = check_fidelity(count_dish, fidelity_rules)
    print(fidelity)
    if fidelity["matched"]:
        return {
            "should_apply": True,
            **fidelity
        }

    return {
        "should_apply": False,
        "reduction": 0
    }

def apply_reduction(pun_initial, quantity, reduction_value):
    pun_initial = Decimal(pun_initial)
    quantity = Decimal(quantity)

    # Case "U50"
    if isinstance(reduction_value, str) and 'U' in reduction_value:
        value = Decimal(re.search(r'\d+', reduction_value).group())
        pun_reduit = pun_initial - value
        total = pun_reduit * quantity

    # Case "F100"
    elif isinstance(reduction_value, str) and 'F' in reduction_value:
        value = Decimal(re.search(r'\d+', reduction_value).group())
        total = (pun_initial * quantity) - value
        pun_reduit = total / quantity  # unit price after global reduction

    # Case percentage (50)
    else:
        reduction_value = Decimal(reduction_value)
        pun_reduit = pun_initial - (pun_initial * reduction_value / 100)

        if quantity > 1:
            total = pun_reduit + ((quantity - 1) * pun_initial)
        else:
            total = pun_reduit

    return round(pun_reduit, 2), round(total, 2)


def create_dynamic_model_class(model_name, fields, db_tables):
    """
    Create a dynamic Django model class.

    Args:
    - model_name (str): The name of the dynamic model.
    - fields (dict): A dictionary specifying the fields of the dynamic model.

    Returns:
    - model_class: The dynamically created Django model class.
    """

    class Meta:
        db_table = db_tables
        ordering = ('-id',)
        # Replace with your app label

    # Define dynamic fields
    dynamic_fields = {
        '__module__': __name__,
        'Meta': Meta,
    }

    # Add fields to the dynamic class
    for field_name, field_type in fields.items():
        # Use the field_type directly, not as a callable
        dynamic_fields[field_name] = field_type

    # Create the dynamic class using the type function
    model_class = type(model_name, (models.Model,), dynamic_fields)

    return model_class


import django_filters


def create_dynamic_filter(models):
    """
    Create a dynamic filter for a model.

    Args:
    - model: The dynamic Django model.

    Returns:
    - filter_class: The dynamically created filter class.
    """

    class DynamicModelFilter(django_filters.FilterSet):
        class Meta:
            model : models
            fields = '__all__'

    return DynamicModelFilter


from rest_framework import serializers


def create_dynamic_serializer(models):
    """
    Create a dynamic Django REST framework serializer for a model.

    Args:
    - model: The dynamic Django model.

    Returns:
    - serializer_class: The dynamically created serializer class.
    """

    class DynamicModelSerializer(serializers.ModelSerializer):
        class Meta:
            model : models
            fields = '__all__'

    return DynamicModelSerializer


# Example usage:
dynamic_fields = {
    'cash' : models.ForeignKey(Cash, on_delete=models.CASCADE, null=True),
    'patient' : models.ForeignKey(Patient, on_delete=models.CASCADE, null=True, related_name='bills'),
    'phone_number' : models.IntegerField(default=0, null=True, blank=True),
    'bank_card_number' : models.IntegerField(default=0, null=True, blank=True),
    'net_payable' : models.FloatField(default=0.0, null=True, blank=True),
    'bills_amount' : models.FloatField(default=0.0, null=True, blank=True),
    'tva' : models.FloatField(default=0.0, null=True, blank=True),
    'insurance' : models.FloatField(default=0.0, null=True, blank=True),
    'insurance_patient' : models.FloatField(default=0.0, null=True, blank=True),
    'ttc' : models.FloatField(default=0.0, null=True, blank=True),
    'amount_received' : models.IntegerField(default=0, null=True, blank=True),
    'amount_paid' : models.IntegerField(default=0, null=True, blank=True),
    'balance' : models.FloatField(default=0.0, null=True, blank=True),
    'refund' : models.FloatField(default=0.0, null=True, blank=True),
    'patient_balance' : models.FloatField(default=0.0, null=True, blank=True),
    'total_amount' : models.FloatField(default=0.0, null=True, blank=True),
    'additional_info' : models.CharField(max_length=255, null=True, blank=True),
    'bill_type' : models.CharField(choices=BILLS_CHOICES, max_length=255),
    'type_accommodation' : models.CharField( max_length=255, null=True, blank=True),
    'bill_type_hospitalization' : models.CharField( max_length=255, null=True, blank=True),
    'bill_shape' : models.CharField(choices=BILL_SHAPE_CHOICES, max_length=255, null=True, blank=True),
    'payment_method' : models.CharField(max_length=255, null=True, blank=True),
    'patient_name' : models.CharField(max_length=255, null=True, blank=True),
    'cash_code' : models.CharField(max_length=255, null=True, blank=True),
    'cashier_name' : models.CharField(max_length=255, null=True, blank=True),
    'doctor_name' : models.CharField(max_length=255, null=True, blank=True),
    'patient_type' : models.CharField(choices=PATIENT_SHAPE_CHOICES, max_length=255),
    'code' : models.CharField(max_length=255, null=True, blank=True, unique=True),
    'createdAt' : models.DateField(auto_now_add=True, null=True, blank=True),
    'timeAt' : models.TimeField(auto_now_add=True, null=True, blank=True),
    'is_proforma' : models.BooleanField(default=False),
    'updatedAt' : models.DateTimeField(auto_now=True, verbose_name="Updated Date"),
    'deleted' : models.BooleanField(default=False),
    'deletedAt' : models.DateTimeField(auto_now_add=True, verbose_name="Delete Date")
}


def get_back(current, previous):
    
    if previous == 0 or previous is None:
        return 0
    else:
        if current is None:
            current = 0
            result = (current - previous) / previous
            return result
        else:
            result = (current - previous) / previous
            return result


def get_first_date_of_month(year, month):
    """Return the first date of the month.
    Args:
        year (int): Year
        month (int): Month
    Returns:
        date (datetime): First date of the current month
    """
    first_date = datetime(year, month, 1)
    return first_date.strftime("%Y-%m-%d")


def get_last_date_of_month(year, month):
    """Return the last date of the month.
    Args:
        year (int): Year, i.e. 2022
        month (int): Month, i.e. 1 for January
    Returns:
        date (datetime): Last date of the current month
    """
    if month == 12:
        last_date = datetime(year, month, 31)
    else:
        last_date = datetime(year, month + 1, 1) + timedelta(days=-1)
    return last_date.strftime("%Y-%m-%d")


import shutil
import glob
def backup_all():
    # Paramètres de la base de données
    database_settings = settings.DATABASES['default']
    db_name = database_settings['NAME']
    db_user = database_settings['USER']
    db_password = database_settings['PASSWORD']
    db_host = database_settings['HOST']
    db_port = database_settings['PORT']
    timeDate = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    root = settings.DBBACKUP_ALL
    backup_file = f"backup.sql"
    # Chemin pour sauvegarder le fichier
    backup_file_path = f"{root}/{backup_file}"

    # Commande pg_dump
    pg_dump_cmd = [
        'pg_dump',
        '-U', db_user,
        '-h', db_host,
        '-p', str(db_port),
        '-d', db_name,
        '-f', backup_file_path
    ]

    # Exécuter la commande pg_dump
    subprocess.run(pg_dump_cmd)

    with open(backup_file_path, 'r', encoding='ISO-8859-1') as source_file:
        content = source_file.read()

    # Write content to the destination file
    file_name = f"backup_{timeDate}.sql"
    chemin_complet = os.path.join(root, file_name)
    with open(chemin_complet.replace('\\', '/'), 'w', encoding='ISO-8859-1') as destination_file:
        destination_file.write(content)


def link_callback(uri, rel):
    """
    Convert HTML URIs to absolute system paths so xhtml2pdf can access those
    resources
    """
    sUrl = settings.STATIC_URL  # Typically /static/
    sRoot = settings.STATIC_ROOT  # Typically /home/userX/project_static/
    mUrl = settings.MEDIA_URL  # Typically /media/
    mRoot = settings.MEDIA_ROOT  # Typically /home/userX/project_static/media/

    if uri.startswith(mUrl):
        path = os.path.join(mRoot, uri.replace(mUrl, ""))
    elif uri.startswith(sUrl):
        path = os.path.join(sRoot, uri.replace(sUrl, ""))
    else:
        return uri

    # make sure that file exists
    if not os.path.isfile(path):
        raise Exception(
            'media URI must start with %s or %s' % (sUrl, mUrl)
        )
    return path


def archive(date_month, user):
    #    Récupérer la date du dernier jour du mois précédent
    if date_month:
        year = str(date_month.split("-")[0])
        month = str(date_month.split("-")[1])
        start_day_of_prev_month = get_first_date_of_month(year=int(date_month.split("-")[0]),
                                                          month=int(date_month.split("-")[1]))
        last_day_of_prev_month = get_last_date_of_month(year=int(date_month.split("-")[0]),
                                                        month=int(date_month.split("-")[1]))
    else:
        # Récupérer la date du premier jour du mois précédent
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        # Récupérer la date du premier jour du mois précédent
        start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        previous_month = str(timezone.now().replace(day=1) - timezone.timedelta(days=1)).rsplit('-', 1)[0]
        year = str(previous_month).rsplit('-', 1)[0]
        month = str(previous_month).rsplit('-', 1)[1]
    # Nom de la table à archiver
    table_name = 'bills'

    table_archive = f"{table_name}_{year}_{month}_archive"
    # Créer une nouvelle table d'archive (en utilisant la structure de la table existante)
    with connections['default'].cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_archive} AS TABLE {table_name} WITH NO DATA")
    # Copier les données à archiver dans la table d'archive

    with connections['default'].cursor() as cursor:
        # cursor.execute(
        #     f"SELECT * FROM {table_name} WHERE \"createdAt\" BETWEEN \'{start_day_of_prev_month}\' AND \'{last_day_of_prev_month}\'")
        # data = cursor.fetchall()
        # print(data)
        try:
            cursor.execute(
                f"INSERT INTO {table_archive} SELECT * FROM {table_name} WHERE \"createdAt\" BETWEEN \'{start_day_of_prev_month}\' AND \'{last_day_of_prev_month}\'")
        except Exception as ex:
            print(ex)
    # Supprimer les données archivées de la table principale
    number_bills = Bills.objects.filter(createdAt__range=[start_day_of_prev_month, last_day_of_prev_month]).count()

    archive = Archive.objects.create(user_id=user.id, month=month, year=year, number_bills=number_bills)

    # # Attempt to delete the file
    # os.remove(last_backup_file)

    Bills.objects.filter(createdAt__range=[start_day_of_prev_month, last_day_of_prev_month]).delete()
    logging.getLogger('info_file').error(
        msg=f"Data archived successfully.")
    return archive


def backup(user):
    call_command('dbbackup')
    backup_path = os.path.join(settings.DBBACKUP_STORAGE_FOLDER, '*.sql')
    #
    # Get a list of all backup files in the directory
    backup_files = glob.glob(backup_path)

    # Check if there are any backup files
    # Get the last modified backup file
    last_backup_file : max(backup_files, key=os.path.getmtime)
    shutil.copy(last_backup_file, settings.BACKUP_STORAGE_FOLDER)
    backup = BackupFile.objects.create(user_id=user.id, file=last_backup_file)
    file_delete = f"{backup.file}"
    os.remove(file_delete.replace('\\', '/'))


def restore():
    backup_path = os.path.join(settings.DBBACKUP_STORAGE_FOLDER, '*.sql')
    #
    # Get a list of all backup files in the directory
    backup_files = glob.glob(backup_path)

    # Check if there are any backup files
    # Get the last modified backup file
    last_backup_file : max(backup_files, key=os.path.getmtime)
    # call_command('dbrestore', '--noinput')


def get_archive(year, month, filter_data):
    filter_data = filter_data
    # if 'start_date' in filter_data and 'end_date' in filter_data:
    #
    #     new_filter_data = filter_data.copy()
    #     date_table = list()
    #     date_table.append(filter_data.get('start_date'))
    #     date_table.append(filter_data.get('end_date'))
    #     new_filter_data['createdAt__range'] = date_table
    #     del new_filter_data['start_date']
    #     del new_filter_data['end_date']
    #     print(new_filter_data['createdAt__range'])
    #
    #     final_filter_data = new_filter_data
    # else:
    #     final_filter_data=filter_data
    # Récupérer la date du dernier jour du mois précédent
    # Nom de la table à archiver
    table_name = 'Bills'
    db_table_name = 'bills'

    table_archive = f"{table_name}_{year}_{month}_archive"
    db_table_archive = f"{db_table_name}_{year}_{month}_archive"
    DynamicModel = create_dynamic_model_class(table_archive, dynamic_fields, db_table_archive)
    # Run migrations for the dynamic model
    call_command('makemigrations')
    # call_command('migrate')

    # Example usage:
    DynamicModelFilter = create_dynamic_filter(DynamicModel)

    # Create an instance of DynamicModel
    # instance = DynamicModel(name='John Doe', age=25, email='john@example.com')
    # instance.save()
    if 'start_date' in filter_data and 'end_date' in filter_data:
        new_filter_data = filter_data.copy()
        queryset_date = DynamicModel.objects.filter(
            createdAt__range=[filter_data.get('start_date'), filter_data.get('end_date')])
        del new_filter_data['start_date']
        del new_filter_data['end_date']
        del new_filter_data['page']
        del new_filter_data['size']
        final_filter_data = new_filter_data
        queryset = queryset_date.filter(**new_filter_data)
    else:
        final_filter_data = filter_data
        queryset = DynamicModel.objects.filter(**filter_data)
    # Use the filter to filter queryset
    # queryset = DynamicModel.objects.filter(**final_filter_data, createdAt__range=[])
    # queryset = DynamicModel.objects.filter(**final_filter_data)

    # Apply the filter to the queryset
    filtered_queryset = DynamicModelFilter(final_filter_data, queryset=queryset).qs
    sum_ca = filtered_queryset.aggregate(Sum('bills_amount'))['bills_amount__sum']
    sum_unpaid = filtered_queryset.aggregate(Sum('balance'))['balance__sum']
    sum_insurance = filtered_queryset.aggregate(Sum('insurance'))['insurance__sum']
    sum_paid = filtered_queryset.aggregate(Sum('amount_paid'))['amount_paid__sum']

    # Print the filtered data
    # Example usage:
    DynamicModelSerializer = create_dynamic_serializer(DynamicModel)

    # # Create an instance of DynamicModel
    # instance = DynamicModel(name='John Doe', age=25, email='john@example.com')
    # instance.save()

    # Serialize the instance
    serializer = DynamicModelSerializer(filtered_queryset, many=True)

    content = {'content': serializer.data, 'sum_ca': sum_ca, 'sum_unpaid': sum_unpaid,'sum_insurance': sum_insurance, 'sum_paid': sum_paid}
    #
    # with connections['default'].cursor() as cursor:
    #     cursor.execute(
    #         f"SELECT * FROM {table_archive} WHERE \"createdAt\" BETWEEN \'{start_date}\' AND \'{last_date}\'")
    #     data = cursor.fetchall()
    return content


def delete_archive(year, month, file):
    table_name = 'bills'

    table_archive = f"{table_name}_{year}_{month}_archive"
    with connections['default'].cursor() as cursor:
        cursor.execute(f"DROP TABLE {table_archive}")
