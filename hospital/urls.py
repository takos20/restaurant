from django.urls import path
from django.conf.urls import include
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)
from drf_spectacular.views import SpectacularAPIView
from rest_framework import routers
from hospital.views import DetailsBillsIngredientViewSet, DetailsInventoryViewSet, DetailsPatientAccountViewSet, DetailsStock_movementViewSet, MovementStockViewSet, PatientAccountViewSet, DistrictViewSet, Stock_movementViewSet, StockViewSet, Storage_depotsViewSet, Type_patientViewSet, UserViewSet, HospitalViewSet, home_view, profile_view, \
    InsuranceViewSet, PatientViewSet, \
    Expenses_natureViewSet, \
    CashViewSet, Cash_movementViewSet, CategoryViewSet, \
    SuppliersViewSet, SuppliesViewSet, BillViewSet, DetailsBillsViewSet, \
    PatientSettlementViewSet, InventoryViewSet, \
     RegionViewSet, CityViewSet, user_logout, LoginView, \
    RefreshTokenView, ArchiveViewSet, BackupFileViewSet, \
     DeliveryInfoViewSet, EventInfoViewSet,CateringInfoViewSet , DetailsSuppliesViewSet

router = routers.DefaultRouter(trailing_slash=False)
# router.register(r'modules', ModuleViewSet)
router.register(r'insurances', InsuranceViewSet)
router.register(r'archives', ArchiveViewSet)
router.register(r'backupfiles', BackupFileViewSet)
router.register(r'bills', BillViewSet, basename='bills')
router.register(r'patients_settlements', PatientSettlementViewSet)
router.register(r'users', UserViewSet)
router.register(r'hospitals', HospitalViewSet)
router.register(r'type_patients', Type_patientViewSet)
router.register(r'patients', PatientViewSet)
router.register(r'expenses_nature', Expenses_natureViewSet)
router.register(r'cashs', CashViewSet)
router.register(r'cash_movements', Cash_movementViewSet)
router.register(r'categories', CategoryViewSet)
router.register(r'regions', RegionViewSet)
router.register(r'cities', CityViewSet)
router.register(r'districts', DistrictViewSet)
router.register(r'suppliers', SuppliersViewSet)
router.register(r'patient_accounts', PatientAccountViewSet)
router.register(r'details_patient_accounts', DetailsPatientAccountViewSet)
router.register(r'supplies', SuppliesViewSet)
router.register(r'details_supplies', DetailsSuppliesViewSet)
router.register(r'details_bills', DetailsBillsViewSet)
router.register(r'deliveries', DeliveryInfoViewSet)
router.register(r'events', EventInfoViewSet)
router.register(r'caterings', CateringInfoViewSet)
router.register(r'details_bills_ingredients', DetailsBillsIngredientViewSet)
router.register(r'movement_stocks', MovementStockViewSet)
router.register(r'inventories', InventoryViewSet)
router.register(r'details_inventories', DetailsInventoryViewSet)
router.register(r'stock_movements', Stock_movementViewSet)
router.register(r'details_stock_movement', DetailsStock_movementViewSet)
router.register(r'storage_depots', Storage_depotsViewSet)
router.register(r'details_stocks', StockViewSet)


urlpatterns = [
    path('', home_view),
    path('api/v1/', include(router.urls)),
    path('api/v1/profile', profile_view),
    path('api/v1/refresh', RefreshTokenView.as_view(), name='refresh_token'),
    path('api/v1/login', LoginView.as_view()),
    path('api/v1/logout', user_logout, name='logout'),  # override sjwt stock token
    path('api/v1/schema', SpectacularAPIView.as_view(), name='schema'),
    # path('api/v1/refresh', TokenRefreshView.as_view(), name='token_refresh'),
]

