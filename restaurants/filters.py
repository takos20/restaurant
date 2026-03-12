import django_filters

from hospital.models import ComposeIngredient, ComposePreparation, DetailsComposeIngredient, DetailsComposePreparation, Dish, DishPreparation, Ingredient, Promotion, PromotionAction, PromotionRule, RecipeIngredient, StructureArticle
from django.utils import timezone
from datetime import datetime, time

class IngredientFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    is_delivery = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    stock_quantity_lt = django_filters.CharFilter(field_name='stock_quantity', lookup_expr=('lt'))
    unit = django_filters.CharFilter(lookup_expr='icontains')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = Ingredient
        fields = {'id': ['exact']}


class DishFilter(django_filters.FilterSet):
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    is_delivery = django_filters.BooleanFilter(lookup_expr='exact')
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    category = django_filters.CharFilter(field_name='category__id', lookup_expr='exact')

    class Meta:
        model = Dish
        fields = {'id': ['exact']}

class ComposeIngredientFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')

    class Meta:
        model = ComposeIngredient
        fields = {'id': ['exact']}


class PromotionFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    start_date  = django_filters.CharFilter(lookup_expr='icontains')
    end_date   = django_filters.CharFilter(lookup_expr='icontains')
    priority    = django_filters.CharFilter(lookup_expr='icontains')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    cumulative = django_filters.DateFilter(lookup_expr='exact')
    class Meta:
        model = Promotion
        fields = {'id': ['exact']}


class PromotionActionFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    action_type = django_filters.CharFilter(lookup_expr='icontains')
    value = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    promotion = django_filters.CharFilter(field_name='promotion__id', lookup_expr='exact')

    class Meta:
        model = PromotionAction
        fields = {'id': ['exact']}


class PromotionRuleFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    category = django_filters.CharFilter(field_name='category__id', lookup_expr='exact')
    promotion = django_filters.CharFilter(field_name='promotion__id', lookup_expr='exact')
    dish = django_filters.CharFilter(field_name='dish__id', lookup_expr='exact')
    type_patient = django_filters.CharFilter(field_name='type_patient__id', lookup_expr='exact')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    dish = django_filters.CharFilter(field_name='dish_id', lookup_expr='exact')

    class Meta:
        model = PromotionRule
        fields = {'id': ['exact']}


class RecipeIngredientFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    dish = django_filters.CharFilter(field_name='dish__id', lookup_expr='exact')
    recipes = django_filters.CharFilter(field_name='recipes__id', lookup_expr='exact')
    compose_preparation = django_filters.CharFilter(field_name='compose_preparation__id', lookup_expr='exact')

    class Meta:
        model = RecipeIngredient
        fields = {'id': ['exact']}

class RecipesFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    dish = django_filters.CharFilter(field_name='dish__id', lookup_expr='exact')
    recipes = django_filters.CharFilter(field_name='recipes__id', lookup_expr='exact')
    compose_preparation = django_filters.CharFilter(field_name='compose_preparation__id', lookup_expr='exact')

    class Meta:
        model = RecipeIngredient
        fields = {'id': ['exact']}


class DetailsComposeIngredientFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    compose_ingredient = django_filters.CharFilter(field_name='compose_ingredient__id', lookup_expr='exact')
    ingredient = django_filters.CharFilter(field_name='ingredient__id', lookup_expr='exact')

    class Meta:
        model = DetailsComposeIngredient
        fields = {'id': ['exact']}

class StructureArticleFilter(django_filters.FilterSet):
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    price = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    dish = django_filters.CharFilter(field_name='dish__id', lookup_expr='exact')
    is_active = django_filters.BooleanFilter(lookup_expr='exact')
    dish_name = django_filters.CharFilter(method='filter_dish_name')


    def filter_dish_name(self, queryset, name, value):
        return queryset.filter(
            dish__name_language__icontains=value
        )


    class Meta:
        model = StructureArticle
        fields = {'id': ['exact']}

class DetailsComposePreparationFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    compose_preparation = django_filters.CharFilter(field_name='compose_preparation__id', lookup_expr='exact')
    compose_ingredient = django_filters.CharFilter(field_name='compose_ingredient__id', lookup_expr='exact')
    ingredient = django_filters.CharFilter(field_name='ingredient__id', lookup_expr='exact')

    class Meta:
        model = DetailsComposePreparation
        fields = {'id': ['exact']}


class ComposePreparationFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    compose_ingredient_name = django_filters.CharFilter(method='filter_compose_ingredient_name')

    class Meta:
        model = ComposePreparation
        fields = {'id': ['exact']}

    def filter_compose_ingredient_name(self, queryset, name, value):
        return queryset.filter(
            compose_ingredient__name_language__icontains=value
        )


class DishPreparationFilter(django_filters.FilterSet):
    is_shared = django_filters.BooleanFilter(lookup_expr='exact')
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    code = django_filters.CharFilter(lookup_expr='icontains')
    name_language = django_filters.CharFilter(lookup_expr='icontains')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    dish = django_filters.CharFilter(field_name='dish__id', lookup_expr='exact')
    deleted = django_filters.BooleanFilter(lookup_expr='exact')

    class Meta:
        model = DishPreparation
        fields = {'id': ['exact']}