import type { Metadata } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";

import { AppShell } from "@/components/app-shell";
import "./globals.css";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "RedKid Intelligence Dashboard",
  description: "İstihbarat ve ilişki analizi komuta paneli",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="tr" className={`${inter.variable} ${jetbrainsMono.variable} h-full antialiased`} data-density="comfortable">
      <body className="min-h-full bg-[var(--bg-base)] font-sans text-[var(--text-primary)]">
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
