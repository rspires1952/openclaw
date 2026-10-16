import { expect, it, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import { mountSolid } from "../../test-helpers/solid-render.tsx";
import { createAgentViewTestProps } from "./agents-view.test-helpers.ts";
import { Agents } from "./view.tsx";

it("opens agent avatar selection from the keyboard and respects the busy state", async () => {
  const container = document.createElement("div");
  container.style.cssText = "position:fixed;inset:0;overflow:auto";
  document.body.append(container);
  const props = createAgentViewTestProps();
  const mounted = mountSolid(Agents, props, container);
  try {
    const editAvatar = page.getByRole("button", { name: "Edit avatar", exact: true });
    const avatarButton = editAvatar.element();
    if (!(avatarButton instanceof HTMLButtonElement)) {
      throw new Error("Avatar edit affordance must use a native button");
    }
    avatarButton.focus();
    await userEvent.keyboard("{Enter}");
    expect(avatarButton.getAttribute("aria-expanded")).toBe("true");
    await page.getByRole("button", { name: "Upload", exact: true }).click();
    const input = container.querySelector<HTMLInputElement>('input[type="file"]');
    if (!input) {
      throw new Error("Missing avatar file input");
    }
    const openPicker = vi.spyOn(input, "click").mockImplementation(() => {});
    const upload = page.getByRole("button", { name: "Choose image…", exact: true });
    const button = upload.element();
    if (!(button instanceof HTMLButtonElement)) {
      throw new Error("Avatar upload must use a native button");
    }
    button.focus();
    expect(document.activeElement).toBe(button);
    await userEvent.keyboard("{Enter}");
    await userEvent.keyboard(" ");
    expect(openPicker).toHaveBeenCalledTimes(2);

    mounted.update({ ...props, overview: { ...props.overview, identitySaving: true } });
    expect(button.disabled).toBe(true);
    button.click();
    expect(openPicker).toHaveBeenCalledTimes(2);
  } finally {
    vi.restoreAllMocks();
    mounted.dispose();

    container.remove();
  }
});
