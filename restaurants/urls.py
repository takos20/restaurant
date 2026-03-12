from django.urls import path
from django.conf.urls import include
from rest_framework import routers
from restaurants.views import ComposeIngredientViewSet, ComposePreparationViewSet, DetailsComposeIngredientViewSet, DetailsComposePreparationViewSet, DishPreparationViewSet, DishViewSet, IngredientViewSet, PromotionActionViewSet, PromotionRuleViewSet, PromotionViewSet, RecipeIngredientViewSet, RecipesViewSet, StructureArticleViewSet
router = routers.DefaultRouter(trailing_slash=False)
router.register(r"promotions", PromotionViewSet)
router.register(r"promotion_rules", PromotionRuleViewSet)
router.register(r"promotion_actions", PromotionActionViewSet)
router.register(r"dishes", DishViewSet)
router.register(r"dish_preparations", DishPreparationViewSet)
router.register(r"recipes", RecipesViewSet)
router.register(r"compose_ingredients", ComposeIngredientViewSet)
router.register(r"compose_preparations", ComposePreparationViewSet)
router.register(r"recipe_ingredients", RecipeIngredientViewSet)
router.register(r"details_compose_ingredients", DetailsComposeIngredientViewSet)
router.register(r"details_compose_preparations", DetailsComposePreparationViewSet)
router.register(r"ingredients", IngredientViewSet)
router.register(r"structure_articles", StructureArticleViewSet)
urlpatterns = [
    path('api/v1/', include(router.urls)),
]
