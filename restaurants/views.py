from decimal import Decimal
from hospital.filters import RecipesFilter
from hospital.forms import RecipesForm
from rest_framework import viewsets

from hospital.helpers import checkBool, checkContent, checkNumber, destocker
from hospital.models import Category, CategoryTranslation, ComposeIngredient, ComposeIngredientTranslation, ComposePreparation, ComposePreparationTranslation, DetailsComposeIngredient, DetailsComposePreparation, Dish, DishPreparation, DishTranslation, Ingredient, IngredientTranslation, Promotion, PromotionAction, PromotionRule, PromotionTranslation, RecipeIngredient, Recipes, StructureArticle, User
from hospital.serializers import ComposeIngredientSerializer, ComposePreparationSerializer, DetailsComposeIngredientSerializer, DetailsComposePreparationSerializer, DishSerializer, IngredientSerializer, PromotionActionSerializer, PromotionRuleSerializer, PromotionSerializer, RecipeIngredientSerializer, DishPreparationSerializer, RecipesSerializer, StructureArticleSerializer
from restaurants.filters import ComposeIngredientFilter, ComposePreparationFilter, DetailsComposeIngredientFilter, DetailsComposePreparationFilter, DishFilter, DishPreparationFilter, IngredientFilter, PromotionActionFilter, PromotionFilter, PromotionRuleFilter, RecipeIngredientFilter, StructureArticleFilter
from restaurants.forms import ComposeIngredientForm, ComposePreparationForm, DetailsComposeIngredientForm, DetailsComposePreparationForm, DishForm, DishPreparationForm, IngredientForm, PromotionActionForm, PromotionForm, PromotionRuleForm, RecipeIngredientForm, StructureArticleForm
from rest_framework.decorators import action, permission_classes, api_view
from django.shortcuts import render
from collections import OrderedDict
from itertools import chain
from django.db.models import F, ForeignKey, Sum, Avg, Max, Count
from django_filters import rest_framework as filters
from rest_framework import viewsets
from rest_framework.permissions import AllowAny, IsAuthenticated, DjangoModelPermissions
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from globals.pagination import CustomPagination
from rest_framework import status
import pandas as pd

class DishViewSet(viewsets.ModelViewSet):
    queryset = Dish.objects.prefetch_related("prices")
    serializer_class = DishSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = DishFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_serializer_context(self):
        return super().get_serializer_context()

    # def get_queryset(self):
    #     user = self.request.user

    #     qs = (
    #         Dish.objects
    #     )

    #     if user.hospital_id:
    #         qs = qs.all(hospital_id=user.hospital_id)

    #     return qs

    
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = DishForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            # obj.hospital = self.request.user.hospital
            obj.save()
            for translate in self.request.data['name_language']:
                get_translate = DishTranslation.objects.filter(dish_id=obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    DishTranslation.objects.create(user=self.request.user, dish_id=obj.id, language=translate['language'], name = translate['name'])

            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = DishForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            for translate in self.request.data['name_language']:
                get_translate = DishTranslation.objects.filter(dish_id=obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    DishTranslation.objects.create(user=self.request.user, dish_id=obj.id, language=translate['language'], name = translate['name'])

            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_dishes(self, request, *args, **kwargs):

        queryset = self.filter_queryset(
            Dish.objects.select_related('recipes').filter(is_active=True)
        )
        serializer = self.get_serializer(queryset, many=True)
        return Response({'content': serializer.data})
    
    @action(detail=False, methods=['get'], url_path='statistics')
    def statistics(self, request, *args, **kwargs):
        language = request.LANGUAGE_CODE
        # For printing results
        dish_category = (
            Dish.objects
            .filter(category__translations__language=language)
            .values(type=F('category__translations__name'))
            .annotate(total=Count('id', distinct=True))
            .order_by('type')
        )
        

        content = {'content': dish_category}
        return Response(data=content, status=status.HTTP_200_OK)
 
    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Dish.objects.filter(translations__name__icontains=dbframe.Nom_fr).last()
                    if get_obj:
                        pass
                    else:
                        get_obj = Dish.objects.filter(translations__name__icontains=dbframe.Nom_en).last()
                    
                    langue = [{"name": checkContent(content=dbframe.Nom_fr), "saved": False, "language": "fr"}, {"name": checkContent(content=dbframe.Nom_en), "saved": False, "language": "en"}]

                    if get_obj is None:
                        if dbframe.Categorie:
                            print(dbframe.Categorie)
                            get_category = Category.objects.filter(code__icontains = dbframe.Categorie).last()
                            print(get_category)
                            obj = Dish.objects.create(name_language=langue, price = checkContent(content=dbframe.Prix),preparation_time = checkNumber(dbframe.Temps_preparation), category = get_category, is_delivery = checkBool(content=dbframe.Remise))
                        else:
                            obj = Dish.objects.create(name_language=langue, price = checkContent(content=dbframe.Prix), preparation_time = checkNumber(dbframe.Temps_preparation), is_delivery = checkBool(content=dbframe.Remise))

                        for translate in langue:
                            get_translate = DishTranslation.objects.filter(dish_id=obj.id, language=translate['language']).last()
                            if get_translate:
                                get_translate.name = translate['name']
                                get_translate.save()
                            else:
                                DishTranslation.objects.create(user=self.request.user, dish_id=obj.id, language=translate['language'], name = translate['name'])

                    else:
                        for translate in langue:
                            get_translate = DishTranslation.objects.filter(dish_id=get_obj.id, language=translate['language']).last()
                            if get_translate:
                                get_translate.name = translate['name']
                                get_translate.save()
                            else:
                                DishTranslation.objects.create(user=self.request.user, dish_id=get_obj.id, language=translate['language'], name = translate['name'])


        return Response(status=status.HTTP_200_OK)


    @action(detail=False, methods=['post'], url_path='name/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'name' in data:
            obj = Dish.objects.filter(translations__name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    
    @action(detail=False, methods=['post'], url_path='translate/exists', permission_classes=[AllowAny])
    def check_translate(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'title' in data:
            obj = DishTranslation.objects.filter(name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['post', 'get'], url_path='sync_product_to_hospital')
    def sync_product_to_hospital(self, request, *args, **kwargs):
        
        try:
            product = self.request.query_params.get(product)
            if product:
                product = Product.objects.get(id=self.request.query_params.get(product))
                if product.is_shared:
                    hospital = self.request.query_params.get(hospital)
                    if hospital:
                        Product.objects.update_or_create(
                                name=product.name,
                                hospital=hospital,
                                defaults={
                                    'price': product.price,
                                    # Copie d'autres champs ici
                                }
                            )

                    else:

                        for hospital in Hospital.objects.exclude(id=product.hospital_id):
                            Product.objects.update_or_create(
                                name=product.name,
                                hospital=hospital,
                                defaults={
                                    'price': product.price,
                                    # Copie d'autres champs ici
                                }
                            )
            else:
                products = Product.objects.filter(deleted=False)
                hospital = self.request.query_params.get(hospital)
                if hospital:
                    for product in products:
                        if product.is_shared:
                            Product.objects.update_or_create(
                                    name=product.name,
                                    hospital=hospital,
                                    defaults={
                                        'price': product.price,
                                        # Copie d'autres champs ici
                                    }
                                )
                else:
                    for product in products:
                        if product.is_shared:
                            for hospital in Hospital.objects.exclude(id=product.hospital_id):
                                Product.objects.update_or_create(
                                    name=product.name,
                                    hospital=hospital,
                                    defaults={
                                        'price': product.price,
                                        # Copie d'autres champs ici
                                    }
                                )
                    
            return Response(status=status.HTTP_200_OK)
        except Exception as e:
            print("Error in creating", e)
            return Response(data=e, status=status.HTTP_404_NOT_FOUND)

class PromotionViewSet(viewsets.ModelViewSet):
    queryset = Promotion.objects.filter(deleted=False)
    serializer_class = PromotionSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = PromotionFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            Promotion.objects
            .select_related('hospital')
            .filter(deleted=False)   # très important
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = PromotionForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            for translate in self.request.data['name_language']:
                get_translate = PromotionTranslation.objects.filter(hospital = self.request.user.hospital, promotion_id=obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    PromotionTranslation.objects.create(hospital = self.request.user.hospital, user=self.request.user, promotion_id=obj.id, language=translate['language'], name = translate['name'])

            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = PromotionForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            for translate in self.request.data['name_language']:
                get_translate = PromotionTranslation.objects.filter(hospital = self.request.user.hospital, promotion_id=obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    PromotionTranslation.objects.create(hospital = self.request.user.hospital, user=self.request.user, promotion_id=obj.id, language=translate['language'], name = translate['name'])

            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='statistics')
    def statistics(self, request, *args, **kwargs):
        # For printing results
        dish_category = Promotion.objects.filter(hospital = self.request.user.hospital).values(type=F('name')).annotate(
            total=Count('id'))
        

        content = {'content': dish_category}
        return Response(data=content, status=status.HTTP_200_OK)
 
    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Dish.objects.filter(title__icontains=dbframe.Name).last()
                    if get_obj:
                        get_obj.name = dbframe.Name
                        get_obj.save()
                    else:
                        Dish.objects.create(hospital = self.request.user.hospital,title=dbframe.Name)
        return Response(status=status.HTTP_200_OK)


    @action(detail=False, methods=['post'], url_path='name/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'name' in data:
            obj = Promotion.objects.filter(hospital = self.request.user.hospital, translations__name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    


class PromotionRuleViewSet(viewsets.ModelViewSet):
    queryset = PromotionRule.objects.filter(deleted=False)
    serializer_class = PromotionRuleSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = PromotionRuleFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            PromotionRule.objects
            .select_related('hospital')
            .filter(deleted=False)   # très important
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = PromotionRuleForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = PromotionRuleForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Dish.objects.filter(title__icontains=dbframe.Name).last()
                    if get_obj:
                        get_obj.name = dbframe.Name
                        get_obj.save()
                    else:
                        Dish.objects.create(hospital = self.request.user.hospital,title=dbframe.Name)
        return Response(status=status.HTTP_200_OK)


class PromotionActionViewSet(viewsets.ModelViewSet):
    queryset = PromotionAction.objects.filter(deleted=False)
    serializer_class = PromotionActionSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = PromotionActionFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            PromotionAction.objects
            .select_related('hospital')   # très important
            .filter(deleted=False)
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = PromotionActionForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = PromotionActionForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Dish.objects.filter(title__icontains=dbframe.Name).last()
                    if get_obj:
                        get_obj.name = dbframe.Name
                        get_obj.save()
                    else:
                        Dish.objects.create(hospital = self.request.user.hospital,title=dbframe.Name)
        return Response(status=status.HTTP_200_OK)

class PromotionActionViewSet(viewsets.ModelViewSet):
    queryset = PromotionAction.objects.filter(deleted=False)
    serializer_class = PromotionActionSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = PromotionActionFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            PromotionAction.objects
            .select_related('hospital')   # très important
            .filter(deleted=False)
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = PromotionActionForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = PromotionActionForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Dish.objects.filter(title__icontains=dbframe.Name).last()
                    if get_obj:
                        get_obj.name = dbframe.Name
                        get_obj.save()
                    else:
                        Dish.objects.create(hospital = self.request.user.hospital,title=dbframe.Name)
        return Response(status=status.HTTP_200_OK)


class DishPreparationViewSet(viewsets.ModelViewSet):
    queryset = DishPreparation.objects.filter(deleted=False)
    serializer_class = DishPreparationSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = DishPreparationFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            DishPreparation.objects
            .select_related('hospital') 
            .filter(deleted=False)  # très important
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = DishPreparationForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = DishPreparationForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()

            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        
        for ri in obj.dish.recipe_ingredients.filter(deleted=False):
            total_needed = ri.quantity * obj.quantity
            # if ri.ingredient.stock_quantity < total_needed:
            #     raise ValueError(f"Stock insuffisant pour {ri.ingredient.name}")
            ri.ingredient.stock_quantity += total_needed
            ri.ingredient.save()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Dish.objects.filter(title__icontains=dbframe.Name).last()
                    if get_obj:
                        get_obj.name = dbframe.Name
                        get_obj.save()
                    else:
                        Dish.objects.create(hospital = self.request.user.hospital,title=dbframe.Name)
        return Response(status=status.HTTP_200_OK)

class IngredientViewSet(viewsets.ModelViewSet):
    queryset = Ingredient.objects.prefetch_related("stocks")
    serializer_class = IngredientSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = IngredientFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_serializer_context(self):
        return super().get_serializer_context()

 
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = IngredientForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            for translate in self.request.data['name_language']:
                get_translate = IngredientTranslation.objects.filter(ingredient_id=obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    IngredientTranslation.objects.create(user=self.request.user, ingredient_id=obj.id, language=translate['language'], name = translate['name'])

            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = IngredientForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            for translate in self.request.data['name_language']:
                get_translate = IngredientTranslation.objects.filter(ingredient_id=obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    IngredientTranslation.objects.create(user=self.request.user, ingredient_id=obj.id, language=translate['language'], name = translate['name'])

            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)
  
    @action(detail=False, methods=['post'], url_path='name/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'name' in data:
            obj = Ingredient.objects.filter(translations__name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['post'], url_path='translate/exists', permission_classes=[AllowAny])
    def check_translate(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'title' in data:
            obj = IngredientTranslation.objects.filter(name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    

    @action(detail=False, methods=['get'], url_path='alls')
    def get_all_consult(self, request, *args, **kwargs):
        title_query = self.request.query_params.get("name_language")
        results = []

        main_queryset = self.filter_queryset(self.get_queryset())

        # -----------------------------
        # MAIN QUERYSET
        # -----------------------------
        if title_query:
            main_queryset = main_queryset.filter(
                name_language__icontains=title_query  # adapte à ton champ
            )

        if main_queryset.exists():
            results += self.get_serializer(main_queryset, many=True).data

        # -----------------------------
        # COMPOSE PREPARATION
        # -----------------------------
        compose_ingredient = ComposeIngredient.objects.filter(deleted=False)

        if title_query:
            compose_ingredient = compose_ingredient.filter(
                translations__name__icontains=title_query
            )

        if compose_ingredient.exists():
            results += ComposeIngredientSerializer(
                compose_ingredient, many=True
            ).data
        
        # -----------------------------
        return Response({'content': results}, status=status.HTTP_200_OK)
    @action(detail=False, methods=['get'], url_path='alls_back')
    def get_all_consult_back(self, request, *args, **kwargs):
        title_query = self.request.query_params.get("name_language")
        results = []

        main_queryset = self.filter_queryset(self.get_queryset())

        # -----------------------------
        # MAIN QUERYSET
        # -----------------------------
        if title_query:
            main_queryset = main_queryset.filter(
                name_language__icontains=title_query  # adapte à ton champ
            )

        if main_queryset.exists():
            results += self.get_serializer(main_queryset, many=True).data

        # -----------------------------
        # COMPOSE PREPARATION
        # -----------------------------
        compose_preparation = ComposePreparation.objects.filter(
            hospital=self.request.user.hospital
        )

        if title_query:
            compose_preparation = compose_preparation.filter(
                translations__name__icontains=title_query
            )

        if compose_preparation.exists():
            results += ComposePreparationSerializer(
                compose_preparation, many=True
            ).data

        # -----------------------------
        return Response({'content': results}, status=status.HTTP_200_OK)
    
    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
        
    @action(detail=False, methods=['post'], url_path='upload', permission_classes=[AllowAny])
    def upload_file(self, request, *args, **kwargs):
        if request.FILES:
            if "file" in request.FILES:
                file = request.FILES.get("file")
                # fs = FileSystemStorage()
                # filename = fs.save(file.name, file)
                # uploaded_file_url = fs.url(filename)
                empexceldata = pd.read_excel(file)
                dbframe = empexceldata
                data = {}
                for dbframe in dbframe.itertuples():
                    get_obj = Ingredient.objects.filter(translations__name__icontains=dbframe.Nom_fr).last()
                    if get_obj:
                        pass
                    else:
                        get_obj = Ingredient.objects.filter(translations__name__icontains=dbframe.Nom_en).last()                    
                    langue = [{"name": checkContent(content=dbframe.Nom_fr), "saved": False, "language": "fr"}, {"name": checkContent(content=dbframe.Nom_en), "saved": False, "language": "en"}]

                    if get_obj is None:
                        obj = Ingredient.objects.create(name_language=langue, price_per_unit = checkNumber(content=dbframe.Prix_unit), stock_quantity = checkNumber(content=dbframe.Stock_quantity_gramme),stock_quantity_two = checkNumber(content=dbframe.Stock_quantity), unit = checkContent(content=dbframe.Unite))

                        for translate in langue:
                            get_translate = IngredientTranslation.objects.filter(ingredient_id=obj.id, language=translate['language']).last()
                            if get_translate:
                                get_translate.name = translate['name']
                                get_translate.save()
                            else:
                                IngredientTranslation.objects.create(user=self.request.user, ingredient_id=obj.id, language=translate['language'], name = translate['name'])
                    
                    else:
                        for translate in langue:
                            get_translate = IngredientTranslation.objects.filter(ingredient_id=get_obj.id, language=translate['language']).last()
                            if get_translate:
                                get_translate.name = translate['name']
                                get_translate.save()
                            else:
                                IngredientTranslation.objects.create(user=self.request.user, ingredient_id=get_obj.id, language=translate['language'], name = translate['name'])
                    


        return Response(status=status.HTTP_200_OK)

class StructureArticleViewSet(viewsets.ModelViewSet):
    queryset = StructureArticle.objects.filter(deleted=False)
    serializer_class = StructureArticleSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = StructureArticleFilter
    filter_backends = (filters.DjangoFilterBackend,)

 
    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = StructureArticleForm(request.data)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = StructureArticleForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)
  
    @action(detail=False, methods=['post'], url_path='name/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'name' in data:
            obj = Ingredient.objects.filter(translations__name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['post'], url_path='translate/exists', permission_classes=[AllowAny])
    def check_translate(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'title' in data:
            obj = IngredientTranslation.objects.filter(name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    


class RecipesViewSet(viewsets.ModelViewSet):
    queryset = Recipes.objects.filter(deleted=False)
    serializer_class = RecipesSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = RecipesFilter
    filter_backends = (filters.DjangoFilterBackend,)

    # def get_queryset(self):
    #     user = self.request.user

    #     qs = (
    #         Recipes.objects
    #         .select_related('hospital')   # très important
    #     )

    #     if user.hospital_id:
    #         qs = qs.filter(hospital_id=user.hospital_id)

    #     return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = RecipesForm(request.data)
        if obj_form.is_valid():

            obj = obj_form.save()
            # obj.hospital = self.request.user.hospital
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = RecipesForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    @action(detail=True, methods=['DELETE'], url_path='delete-empty')
    def delete_empty(self, request, pk=None):
        supply = self.get_object()

        has_lines = RecipeIngredient.objects.filter(recipes=supply).exists()

        if has_lines:
            return Response(
                {"detail": "Cannot delete, lines exist."},
                status=status.HTTP_400_BAD_REQUEST
            )

        supply.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='create-empty')
    def create_empty(self, request):
        lines = Recipes.objects.annotate(
            line_count=Count('recipe_ingredients'),
        ).filter(line_count=0, user_id=request.user.id)
        lines.delete()
        
        supply = Recipes.objects.create(
            dish=None,
            total_amount=0,
            user_id = request.user.id
        )
        return Response(data={"id": supply.id}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'], url_path='title/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'name' in data:
            obj = Recipes.objects.filter(translations__name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='current', permission_classes=[AllowAny])
    def check_current(self, request, *args, **kwargs):        
        recipe = RecipeIngredient.objects.order_by('dish_id').distinct('dish_id')

        serializer = self.get_serializer(recipe, many=True)
        content = {'content':serializer.data}
        return Response(data=content, status=status.HTTP_200_OK)    
    

class ComposeIngredientViewSet(viewsets.ModelViewSet):
    queryset = ComposeIngredient.objects.filter(deleted=False)
    serializer_class = ComposeIngredientSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = ComposeIngredientFilter
    filter_backends = (filters.DjangoFilterBackend,)

    # def get_queryset(self):
    #     user = self.request.user

    #     qs = (
    #         ComposeIngredient.objects
    #         .select_related('hospital')   # très important
    #     )

    #     if user.hospital_id:
    #         qs = qs.filter(hospital_id=user.hospital_id)

    #     return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        user=self.request.user
        obj_form = ComposeIngredientForm(request.data)
        if obj_form.is_valid():
            get_compose = ComposeIngredient.objects.filter(id=request.data['compose_ingredient']).last()
            get_compose.name_language = request.data['name_language']
            get_compose.stock_quantity = request.data['stock_quantity']
            get_compose.total_amount = request.data['total_amount']
            if 'price_per_unit' in request.data:
                get_compose.price_per_unit = request.data['price_per_unit']
            get_compose.save()
            get_ingredient = DetailsComposeIngredient.objects.filter(compose_ingredient=get_compose).filter(deleted=False)
            # for ingredient in get_ingredient:
            #     destocker(ingredient.ingredient, ingredient.quantity, get_ingredient.id, hospital = user.hospital, source="SUP DISH")
            # obj = obj_form.save()
            # obj.hospital = self.request.user.hospital
            # obj.save()
            serializer = self.get_serializer(get_compose, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = ComposeIngredientForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    @action(detail=True, methods=['DELETE'], url_path='delete-empty')
    def delete_empty(self, request, pk=None):
        supply = self.get_object()
        has_lines = DetailsComposeIngredient.objects.filter(compose_ingredient=supply).exists()

        if has_lines:
            return Response(
                {"detail": "Cannot delete, lines exist."},
                status=status.HTTP_200_OK
            )

        supply.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='create-empty')
    def create_empty(self, request):
        lines = ComposeIngredient.objects.annotate(
            line_count=Count('compose_ingredients'),
        ).filter(line_count=0, user_id=request.user.id)
        lines.delete()

        supply = ComposeIngredient.objects.create(
            total_amount=0,
            user_id = request.user.id
        )
        return Response(data={"id": supply.id}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'], url_path='name/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"name": ["This field already exists."]}
        if 'name' in data:
            obj = ComposeIngredient.objects.filter(translations__name__icontains=data['name'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='current', permission_classes=[AllowAny])
    def check_current(self, request, *args, **kwargs):

        recipe = RecipeIngredient.objects.order_by('dish_id').distinct('dish_id')

        serializer = self.get_serializer(recipe, many=True)
        content = {'content':serializer.data}
        return Response(data=content, status=status.HTTP_200_OK)    
    

class ComposePreparationViewSet(viewsets.ModelViewSet):
    queryset = ComposePreparation.objects.filter(deleted=False)
    serializer_class = ComposePreparationSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = ComposePreparationFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            ComposePreparation.objects
            .select_related('hospital')
            .filter(deleted=False)   # très important
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        user=self.request.user
        obj_form = ComposePreparationForm(request.data)
        if obj_form.is_valid():
            get_compose = ComposePreparation.objects.filter(id=request.data['compose_preparation'], hospital=user.hospital).last()
            get_compose.stock_quantity += float(request.data['stock_quantity'])
            get_compose.total_amount += Decimal(request.data['total_amount'])
            get_compose.save()
            get_ingredient = DetailsComposePreparation.objects.filter(compose_preparation=get_compose, hospital=user.hospital).filter(deleted=False)
            for ingredient in get_ingredient:
                destocker(user, ingredient.ingredient, ingredient.quantity, get_compose.id, storage_depots = None, hospital = user.hospital, source="SUP DISH")
            # obj = obj_form.save()
            # obj.hospital = self.request.user.hospital
            # obj.save()
            serializer = self.get_serializer(get_compose, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = ComposePreparationForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    @action(detail=True, methods=['DELETE'], url_path='delete-empty')
    def delete_empty(self, request, pk=None):
        compose = self.get_object()
        has_lines = DetailsComposePreparation.objects.filter(compose_preparation=compose, hospital=request.user.hospital,).exists()

        if has_lines:
            return Response(
                {"detail": "Cannot delete, lines exist."},
                status=status.HTTP_200_OK
            )

        compose.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='create-empty')
    def create_empty(self, request):

        lines = ComposePreparation.objects.annotate(
            line_count=Count('compose_preparations'),
        ).filter(line_count=0, user_id=request.user.id, hospital=request.user.hospital)
        lines.delete()

        compose = ComposePreparation.objects.create(
            hospital=request.user.hospital,
            total_amount=0,
            user_id = request.user.id
        )
        return Response(data={"id": compose.id}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'], url_path='title/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"title": ["This field already exists."]}
        if 'title' in data:
            obj = Recipes.objects.filter(title__icontains=data['title'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='current', permission_classes=[AllowAny])
    def check_current(self, request, *args, **kwargs):
        user_hospital = self.request.user.hospital
        if user_hospital:
            recipe = RecipeIngredient.objects.filter(hospital=user_hospital).order_by('dish_id').distinct('dish_id')
        else:

            recipe = RecipeIngredient.objects.order_by('dish_id').distinct('dish_id')

        serializer = self.get_serializer(recipe, many=True)
        content = {'content':serializer.data}
        return Response(data=content, status=status.HTTP_200_OK)    
    

class RecipeIngredientViewSet(viewsets.ModelViewSet):
    queryset = RecipeIngredient.objects.filter(deleted=False)
    serializer_class = RecipeIngredientSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = RecipeIngredientFilter
    filter_backends = (filters.DjangoFilterBackend,)

    # def get_queryset(self):
    #     user = self.request.user

    #     qs = (
    #         RecipeIngredient.objects
    #         .select_related('hospital')   # très important
    #     )

    #     if user.hospital_id:
    #         qs = qs.filter(hospital_id=user.hospital_id)

    #     return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]
    
    def create(self, request, *args, **kwargs):
        obj_form = RecipeIngredientForm(request.data)
        if obj_form.is_valid():
            parts = request.data['item_uid'].split('-')
            get_recipe=RecipeIngredient.objects.filter(dish_id=request.data['dish'], recipes= request.data['recipes'], ingredient_id=int(parts[1])).last()
            if get_recipe:
                get_recipe.quantity=request.data['quantity']
                get_recipe.save()
                obj=get_recipe
            else:
                obj = obj_form.save()
                obj.recipes_id = request.data['recipes']
                obj.save()
            get_ingredient = Ingredient.objects.filter(id=int(parts[1])).last()
            get_recipes = Recipes.objects.filter(id=request.data['recipes']).last()
            get_recipes.dish_id = request.data['dish']
            get_recipes.total_amount += float(request.data['quantity']) * float(get_ingredient.price_per_unit)
            get_recipes.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    # def create(self, request, *args, **kwargs):
    #     obj_form = RecipeIngredientForm(request.data)
    #     get_recipes = Recipes.objects.filter(id = request.data['recipes'], user_id=request.user.id).last()
    #     user = self.request.user
        
    #     if obj_form.is_valid():
    #         queryset = Ingredient.objects.filter(hospital=user.hospital, translations_name__icontains=request.data['name']).last()
    #         querysetCompose = ComposeIngredient.objects.filter(hospital=user.hospital, translations_name__icontains=request.data['name']).last()
            

    #         if queryset:
    #             filters = {
    #                 "dish": request.data['details_bills'],
    #                 "ingredient": request.data['ingredient'],
    #                 "hospital": user.hospital,
    #             }
    #             get_details = RecipeIngredient.objects.filter(**filters).last()
    #             if get_details:

    #                 get_details.quantity = request.data["quantity"]
    #                 get_details.save()
    #                 return Response(status=status.HTTP_201_CREATED)

    #             else:
    #                 obj = obj_form.save()
    #                 obj.hospital = user.hospital
    #                 obj.recipes_id = request.data['recipes']
    #                 obj.save()
                    
    #             get_ingredient = Ingredient.objects.filter(id=request.data['ingredient']).last()
    #             get_recipes.dish_id=request.data['dish']
    #             get_recipes.total_amount += float(Decimal(request.data['quantity']) * get_ingredient.price_per_unit)
    #             get_recipes.save()
    #             serializer = self.get_serializer(obj, many=False)
    #             return Response(data=serializer.data, status=status.HTTP_201_CREATED)
    #         elif querysetCompose:
    #             filters = {
    #                 "dish": request.data['dish'],
    #                 "compose_ingredient": request.data['compose_ingredient'],
    #             }
    #             get_details = RecipeIngredient.objects.filter(**filters).last()
    #             if get_details:

    #                 get_details.quantity = request.data["quantity"]
    #                 get_details.save()
    #                 return Response(status=status.HTTP_201_CREATED)

    #             else:
    #                 obj = obj_form.save()
    #                 obj.hospital = user.hospital
    #                 obj.recipes_id = request.data['recipes']
    #                 obj.save()
                    
    #             get_ingredient = ComposeIngredient.objects.filter(id=request.data['compose_ingredient']).last()
    #             get_recipes.dish_id=request.data['dish']
    #             get_recipes.total_amount += float(Decimal(request.data['quantity']) * get_ingredient.price_per_unit)
    #             get_recipes.save()
    #             serializer = self.get_serializer(obj, many=False)
    #             return Response(data=serializer.data, status=status.HTTP_201_CREATED)
            # get_recipe=RecipeIngredient.objects.filter(dish_id=request.data['dish'], ingredient_id=request.data['ingredient']).last()

            # if get_recipe:
            #     get_recipe.quantity=request.data['quantity']
            #     get_recipe.save()
            #     obj=get_recipe
            # else:

            #     obj = obj_form.save()
            #     obj.recipes_id = request.data['recipes']
            #     obj.hospital = self.request.user.hospital
            #     obj.save()
            
        # errors = {**obj_form.errors}
        # return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = RecipeIngredientForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    @action(detail=True, methods=['DELETE'], url_path='delete-empty')
    def delete_empty(self, request, pk=None):
        supply = self.get_object()

        has_lines = RecipeIngredient.objects.filter(dish=supply).exists()

        if has_lines:
            return Response(
                {"detail": "Cannot delete, lines exist."},
                status=status.HTTP_400_BAD_REQUEST
            )

        supply.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='create-empty')
    def create_empty(self, request):
        lines = Recipes.objects.annotate(
            line_count=Count('recipe_ingredients'),
        ).filter(line_count=0, user_id=request.user.id)
        lines.delete()

        supply = Recipes.objects.create(
            dish=None,
            user_id = request.user.id
        )
        return Response(data={"id": supply.id}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'], url_path='title/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"title": ["This field already exists."]}
        if 'title' in data:
            obj = RecipeIngredient.objects.filter(title__icontains=data['title'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='current', permission_classes=[AllowAny])
    def check_current(self, request, *args, **kwargs):

        recipe = RecipeIngredient.objects.order_by('dish_id').distinct('dish_id')

        serializer = self.get_serializer(recipe, many=True)
        content = {'content':serializer.data}
        return Response(data=content, status=status.HTTP_200_OK)    
   
class DetailsComposeIngredientViewSet(viewsets.ModelViewSet):
    queryset = DetailsComposeIngredient.objects.filter(deleted=False)
    serializer_class = DetailsComposeIngredientSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = DetailsComposeIngredientFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = DetailsComposeIngredientForm(request.data)
        user=request.user
        
        if obj_form.is_valid():
            get_recipe=DetailsComposeIngredient.objects.filter(compose_ingredient = request.data['compose_ingredient'], ingredient_id=request.data['ingredient']).last()

            if get_recipe:
                get_recipe.quantity=request.data['quantity']
                get_recipe.save()
                obj=get_recipe
            else:

                obj = obj_form.save()
                obj.compose_ingredient_id = request.data['compose_ingredient']
                obj.save()
            get_obj = ComposeIngredient.objects.filter(id = request.data['compose_ingredient'], user_id=request.user.id).last()
            for translate in self.request.data['name_language']:
                get_translate = ComposeIngredientTranslation.objects.filter(compose_ingredient_id=get_obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    ComposeIngredientTranslation.objects.create(user=self.request.user, compose_ingredient_id=get_obj.id, language=translate['language'], name = translate['name'])

        
            get_ingredient = Ingredient.objects.filter(id=request.data['ingredient']).last()
            get_obj.total_amount += float(Decimal(request.data['quantity']) * get_ingredient.price_per_unit)
            if 'price_per_unit' in request.data:
                get_obj.price_per_unit = request.data['price_per_unit']
            if 'stock_quantity' in request.data:
                get_obj.stock_quantity = request.data['stock_quantity']
            if 'unit' in request.data:
                get_obj.unit = request.data['unit']
            get_obj.user = user
            get_obj.name_language = request.data['name_language']
            get_obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = DetailsComposeIngredientForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    @action(detail=False, methods=['post'], url_path='title/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"title": ["This field already exists."]}
        if 'title' in data:
            obj = RecipeIngredient.objects.filter(title__icontains=data['title'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='current', permission_classes=[AllowAny])
    def check_current(self, request, *args, **kwargs):

        recipe = RecipeIngredient.objects.order_by('dish_id').distinct('dish_id')

        serializer = self.get_serializer(recipe, many=True)
        content = {'content':serializer.data}
        return Response(data=content, status=status.HTTP_200_OK)    
     
class DetailsComposePreparationViewSet(viewsets.ModelViewSet):
    queryset = DetailsComposePreparation.objects.filter(deleted=False)
    serializer_class = DetailsComposePreparationSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = DetailsComposePreparationFilter
    filter_backends = (filters.DjangoFilterBackend,)

    def get_queryset(self):
        user = self.request.user

        qs = (
            DetailsComposePreparation.objects
            .select_related('hospital')
            .filter(deleted=False)   # très important
        )

        if user.hospital_id:
            qs = qs.filter(hospital_id=user.hospital_id)

        return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'create':
            user = self.request.user
            if isinstance(user, User):
                permission_classes = [IsAuthenticated]
            else:
                permission_classes = [AllowAny]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]

    def create(self, request, *args, **kwargs):
        obj_form = DetailsComposePreparationForm(request.data)
        user=request.user
        
        if obj_form.is_valid():
            get_recipe=DetailsComposePreparation.objects.filter(hospital=user.hospital, compose_preparation = request.data['compose_preparation'], ingredient_id=request.data['ingredient'], user=user).last()

            if get_recipe:
                get_recipe.quantity=request.data['quantity']
                get_recipe.save()
                obj=get_recipe
            else:

                obj = obj_form.save()
                obj.compose_ingredient_id = request.data['compose_ingredient']
                obj.hospital = self.request.user.hospital
                obj.save()
            get_obj = ComposePreparation.objects.filter(id = request.data['compose_preparation'], user_id=request.user.id, hospital=user.hospital).last()
            for translate in get_obj.compose_ingredient.name_language:
                get_translate = ComposePreparationTranslation.objects.filter(hospital = self.request.user.hospital, compose_preparation_id=get_obj.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    name = translate['name']
                    ComposePreparationTranslation.objects.create(hospital = self.request.user.hospital, user=self.request.user, compose_preparation_id=get_obj.id, language=translate['language'], name = name)

        
            get_ingredient = Ingredient.objects.filter(id=request.data['ingredient']).last()
            get_obj.total_amount += float(Decimal(request.data['quantity']) * get_ingredient.price_per_unit)
            if 'price_per_unit' in request.data:
                get_obj.price_per_unit = request.data['price_per_unit']
            if 'stock_quantity' in request.data:
                get_obj.stock_quantity = request.data['stock_quantity']
            if 'unit' in request.data:
                get_obj.unit = request.data['unit']
            get_obj.user = user
            get_obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_201_CREATED)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, *args, **kwargs):
        obj = self.get_object()
        obj_form = DetailsComposePreparationForm(request.data, instance=obj)
        if obj_form.is_valid():
            obj = obj_form.save()
            obj.save()
            serializer = self.get_serializer(obj, many=False)
            return Response(data=serializer.data, status=status.HTTP_200_OK)
        errors = {**obj_form.errors}
        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        obj = self.get_object()
        obj.deleted = True
        obj.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['get'], url_path='all')
    def get_all_exp(self, request, *args, **kwargs):
        
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True, ).data
        content = {'content': serializer}
        # patient_id = self.request.query_params.get("patient_id")
        # get_bills = Bills.objects.filter(patient_id=patient_id).aggregate(Sum('balance'))['balance__sum']
        # content = {'content': {'solde_patient': get_bills}}
        return Response(data=content, status=status.HTTP_200_OK)
    
    @action(detail=True, methods=['DELETE'], url_path='delete-empty')
    def delete_empty(self, request, pk=None):
        supply = self.get_object()

        has_lines = RecipeIngredient.objects.filter(dis=supply, hospital=request.user.hospital,).exists()

        if has_lines:
            return Response(
                {"detail": "Cannot delete, lines exist."},
                status=status.HTTP_400_BAD_REQUEST
            )

        supply.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path='create-empty')
    def create_empty(self, request):
        supply = RecipeIngredient.objects.create(
            hospital=request.user.hospital,
            suppliers=None,
            supply_amount=0,
            user_id = request.user.id
        )
        return Response(data={"id": supply.id}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'], url_path='title/exists', permission_classes=[AllowAny])
    def check_obj(self, request, *args, **kwargs):
        data = request.data
        errors = {"title": ["This field already exists."]}
        if 'title' in data:
            obj = RecipeIngredient.objects.filter(title__icontains=data['title'])
            if obj:
                return Response(status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='current', permission_classes=[AllowAny])
    def check_current(self, request, *args, **kwargs):
        user_hospital = self.request.user.hospital
        if user_hospital:
            recipe = RecipeIngredient.objects.filter(hospital=user_hospital).order_by('dish_id').distinct('dish_id')
        else:

            recipe = RecipeIngredient.objects.order_by('dish_id').distinct('dish_id')

        serializer = self.get_serializer(recipe, many=True)
        content = {'content':serializer.data}
        return Response(data=content, status=status.HTTP_200_OK)    
      
    @action(detail=False, methods=['POST'], url_path='create', permission_classes=[AllowAny])
    def check_create(self, request, *args, **kwargs):
        user = self.request.user
        get_obj = ComposePreparation.objects.filter(compose_ingredient_id = request.data['compose_ingredient']).last()
        if get_obj:
            details = DetailsComposeIngredient.objects.filter(compose_ingredient_id = request.data['compose_ingredient']).filter(deleted=False)
            for detail in details:
                # qty = (request.data['stock_quantity'] * detail.quantity) / detail.compose_ingredient.stock_quantity

                stock_quantity = Decimal(request.data['stock_quantity'])

                qty = (
                    stock_quantity
                    * Decimal(detail.quantity)
                    / Decimal(detail.compose_ingredient.stock_quantity)
                )
                if detail.ingredient.price_per_unit:

                    cost = qty * detail.ingredient.price_per_unit
                else:
                    cost = 0

                get_detail_preparation = DetailsComposePreparation.objects.filter(hospital=user.hospital,compose_preparation_id=get_obj.id, compose_ingredient_id=request.data['compose_ingredient'],ingredient=detail.ingredient).last()
                if get_detail_preparation:
                    get_detail_preparation.quantity += qty
                    get_detail_preparation.cost += cost
                    get_detail_preparation.save()
                else:
                    DetailsComposePreparation.objects.create(hospital=user.hospital,compose_preparation_id=get_obj.id, ingredient=detail.ingredient,compose_ingredient_id=request.data['compose_ingredient'], quantity=qty, user=user, cost=cost)

            get_obj.stock_quantity += float(request.data['stock_quantity'])
            get_obj.save()
        else:
            compose = ComposePreparation.objects.create(
                    hospital=request.user.hospital,
                    total_amount=0,
                    stock_quantity=request.data['stock_quantity'],
                    compose_ingredient_id=request.data['compose_ingredient'],
                    user_id = user.id
                )

            for translate in compose.compose_ingredient.name_language:
                get_translate = ComposePreparationTranslation.objects.filter(hospital = user.hospital, compose_preparation_id=compose.id, language=translate['language']).last()
                if get_translate:
                    get_translate.name = translate['name']
                    get_translate.save()
                else:
                    name = translate['name']
                    ComposePreparationTranslation.objects.create(hospital = user.hospital, user=user, compose_preparation_id=compose.id, language=translate['language'], name = name)

            
            details = DetailsComposeIngredient.objects.filter(compose_ingredient_id = request.data['compose_ingredient']).filter(deleted=False)
            for detail in details:
                # qty = (request.data['stock_quantity'] * detail.quantity) / detail.compose_ingredient.stock_quantity

                stock_quantity = Decimal(request.data['stock_quantity'])

                qty = (
                    stock_quantity
                    * Decimal(detail.quantity)
                    / Decimal(detail.compose_ingredient.stock_quantity)
                )
                if detail.ingredient.price_per_unit:

                    cost = qty * detail.ingredient.price_per_unit
                else:
                    cost = 0

                get_detail_preparation = DetailsComposePreparation.objects.filter(hospital=user.hospital,compose_preparation_id=compose.id,compose_ingredient_id=request.data['compose_ingredient'], ingredient=detail.ingredient).last()
                if get_detail_preparation:
                    get_detail_preparation.quantity = qty
                    get_detail_preparation.cost = cost
                    get_detail_preparation.save()
                else:
                    DetailsComposePreparation.objects.create(hospital=user.hospital,compose_preparation_id=compose.id, ingredient=detail.ingredient,compose_ingredient_id=request.data['compose_ingredient'], quantity=qty, user=user, cost=cost)

        return Response(status=status.HTTP_200_OK)    
    
# class DishPreparationViewSet(viewsets.ModelViewSet):
#     queryset = DishPreparation.objects.filter(deleted=False)
#     serializer_class = DishPreparationSerializer
#     renderer_classes = [JSONRenderer]
#     permission_classes = (IsAuthenticated, DjangoModelPermissions)
#     pagination_class = CustomPagination
#     filterset_class = DishPreparationFilter
#     filter_backends = (filters.DjangoFilterBackend,)

#     def get_queryset(self):
#         user_hospital = self.request.user.hospital
#         if user_hospital:
#             return DishPreparation.objects.filter(hospital=user_hospital)
#         return DishPreparation.objects.filter(deleted=False)

#     def get_permissions(self):
#         """
#         Instantiates and returns the list of permissions that this view requires.
#         """
#         if self.action == 'create':
#             user = self.request.user
#             if isinstance(user, User):
#                 permission_classes = [IsAuthenticated]
#             else:
#                 permission_classes = [AllowAny]
#         else:
#             permission_classes = [IsAuthenticated]
#         return [permission() for permission in permission_classes]

#     def create(self, request, *args, **kwargs):
#         obj_form = DishPreparationForm(request.data)
#         if obj_form.is_valid():
#             obj = obj_form.save()
#             obj.hospital = self.request.user.hospital
#             obj.save()
#             serializer = self.get_serializer(obj, many=False)
#             return Response(data=serializer.data, status=status.HTTP_201_CREATED)
#         errors = {**obj_form.errors}
#         return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

#     def update(self, request, *args, **kwargs):
#         obj = self.get_object()
#         obj_form = DishPreparationForm(request.data, instance=obj)
#         if obj_form.is_valid():
#             obj = obj_form.save()
#             obj.save()
#             serializer = self.get_serializer(obj, many=False)
#             return Response(data=serializer.data, status=status.HTTP_200_OK)
#         errors = {**obj_form.errors}
#         return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

#     def destroy(self, request, *args, **kwargs):
#         obj = self.get_object()
#         obj.deleted = True
#         obj.save()
#         return Response(status=status.HTTP_204_NO_CONTENT)


#     @action(detail=False, methods=['post'], url_path='title/exists', permission_classes=[AllowAny])
#     def check_obj(self, request, *args, **kwargs):
#         data = request.data
#         errors = {"title": ["This field already exists."]}
#         if 'title' in data:
#             obj = DishPreparation.objects.filter(title__icontains=data['title'])
#             if obj:
#                 return Response(status=status.HTTP_200_OK)
#             else:
#                 return Response(status=status.HTTP_404_NOT_FOUND)

#         return Response(data=errors, status=status.HTTP_400_BAD_REQUEST)

