import { useQuery } from "@tanstack/react-query";
import { getFetcher } from "@/lib/utils";

export type ItemDetailsParams = {
  titleId?: string | null;
  nameId?: string | null;
};

export type ItemDetailsResponse = {
  id: string;
  entityType: "title" | "person";
  description: string;
};

const ITEM_DETAILS_QUERY_KEY = "item-details";

function hasValidTitleId(titleId: string | null | undefined): boolean {
  return typeof titleId === "string" && /^tt\d+$/.test(titleId);
}

function hasValidNameId(nameId: string | null | undefined): boolean {
  return typeof nameId === "string" && /^nm\d+$/.test(nameId);
}

function isExactlyOneValidIdentifier(params: ItemDetailsParams): boolean {
  const hasTitleId = hasValidTitleId(params.titleId);
  const hasNameId = hasValidNameId(params.nameId);
  return (hasTitleId && !hasNameId) || (!hasTitleId && hasNameId);
}

export function useItemDetails(params: ItemDetailsParams) {
  const enabled = isExactlyOneValidIdentifier(params);

  return useQuery({
    queryKey: ["query", ITEM_DETAILS_QUERY_KEY, params],
    queryFn: getFetcher<ItemDetailsResponse>("/query/item-details", { params }),
    enabled,
  });
}